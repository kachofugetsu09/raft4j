package site.hnfy258;

import lombok.Getter;
import lombok.Setter;
import site.hnfy258.core.AppendResult;
import site.hnfy258.core.LogEntry;
import site.hnfy258.core.RoleState;
import site.hnfy258.network.RaftNetwork;
import site.hnfy258.rpc.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
@Setter
public class Raft {
    private final int selfId; // 当前节点ID
    private final List<Integer> peerIds; // 所有节点ID列表
    private final RaftNetwork network; // 网络层抽象

    // Raft算法状态
    private int currentTerm; // 当前任期
    private int votedFor; // 当前任期内投票给的候选人ID
    private final List<LogEntry> log; // 日志条目数组
    private RoleState state; // 当前角色状态

    // 所有服务器上的易失性状态
    private int commitIndex; // 已提交的日志条目索引
    private int lastApplied; // 最后应用到状态机的日志条目索引

    // 领导者上的易失性状态
    private final int[] nextIndex; // 每个从节点下一个应当被传递的条目
    private final int[] matchIndex; // 每个从节点已复制的最高日志条目索引

    // 选举超时相关
    private long lastHeartbeatTime;
    private int electionTimeout;
    private final int heartbeatInterval = 200; // 心跳间隔 200ms，适配真实网络

    // 投票统计
    private int voteCount;

    /**
     * -- SETTER --
     *  设置RaftNode引用
     */
    // RaftNode引用，用于控制定时器
    private RaftNode nodeRef;

    private final Object lock = new Object(); // 锁对象，用于同步

    public Raft(int selfId, int[] peerIds, RaftNetwork network) {
        this.selfId = selfId;
        this.network = network;
        this.peerIds = new ArrayList<>();
        for (int peerId : peerIds) {
            this.peerIds.add(peerId);
        }
        this.currentTerm = 0;
        this.votedFor = -1;
        this.log = new ArrayList<>();
        log.add(new LogEntry(-1,-1,null));
        this.state = RoleState.FOLLOWER;
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.nextIndex = new int[peerIds.length];
        this.matchIndex = new int[peerIds.length];

        for(int i=0; i < peerIds.length; i++){
            nextIndex[i] = 1;
            matchIndex[i] = 0;
        }
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.electionTimeout = generateElectionTimeout();
        this.voteCount = 0;
        //todo 读取持久化状态
    }

    private int generateElectionTimeout() {
        // 为真实网络环境优化：选举超时时间为 1000-2000ms 之间的随机值
        return ThreadLocalRandom.current().nextInt(1000, 2000);
    }

    /**
     * 重置选举超时时间
     */
    private void resetElectionTimeout() {
        lastHeartbeatTime = System.currentTimeMillis();
        if (nodeRef != null) {
            nodeRef.resetElectionTimer();
        }
    }

    public void startElection(){
        RequestVoteArg arg = new RequestVoteArg();
        synchronized (lock){
            if (state != RoleState.CANDIDATE){
                lock.notifyAll();
                return;
            }
            this.currentTerm++; // 增加当前任期
            this.votedFor = selfId; // 给自己投票
            this.resetElectionTimeout(); // 重置选举超时

            arg.candidateId = selfId;
            arg.term = currentTerm;
            arg.lastLogIndex = log.size() - 1;
            arg.lastLogTerm = log.get(log.size() - 1).getLogTerm();
        }
        // 异步发送投票请求并统计结果
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        AtomicInteger voteCount = new AtomicInteger(1); // 先给自己投一票

        for (Integer peerId : peerIds) {
            if (peerId == selfId) {
                continue;
            }

            CompletableFuture<Boolean> future = network.sendRequestVote(peerId, arg)
                    .thenApply(reply -> {
                        boolean granted = handleRequestVoteReply(peerId, arg.term, reply);
                        if (granted) {
                            System.out.println("Node " + selfId + " received vote from " + peerId);
                            voteCount.incrementAndGet();
                        }
                        return granted;
                    });
            futures.add(future);
        }

        // 等待所有投票请求完成或达到多数票
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
        );

        // 异步处理投票结果
        allFutures.thenRun(() -> {
            synchronized (lock) {
                int requiredVotes = (peerIds.size() / 2) + 1; // 修正：集群大小的一半加1
                int currentVotes = voteCount.get();
                System.out.println("Node " + selfId + " election result: " + currentVotes + "/" + requiredVotes + " votes, state=" + state);

                if (state == RoleState.CANDIDATE && currentVotes >= requiredVotes) {
                    becomeLeader();
                }
            }
        });

        // 或者当获得多数票时立即成为领导者
        for (CompletableFuture<Boolean> future : futures) {
            future.thenRun(() -> {
                synchronized (lock) {
                    int requiredVotes = (peerIds.size() / 2) + 1; // 修正：集群大小的一半加1
                    int currentVotes = voteCount.get();

                    if (state == RoleState.CANDIDATE && currentVotes >= requiredVotes) {
                        System.out.println("Node " + selfId + " got majority votes: " + currentVotes + "/" + requiredVotes);
                        becomeLeader();
                    }
                }
            });
        }
    }

    public boolean isElectionTimeout() {
        return System.currentTimeMillis() - lastHeartbeatTime > electionTimeout;
    }

    public synchronized boolean handleRequestVoteReply(int serverId,
                                                       int requestTerm,
                                                       RequestVoteReply reply){
        if (reply.term > currentTerm) {
            this.becomeFollower(reply.term);
            return false;
        }

        if(currentTerm != requestTerm){
            return false; // 如果当前任期不匹配，忽略回复
        }

        return reply.voteGranted;
    }

    public synchronized RequestVoteReply handleRequestVoteRequest(RequestVoteArg arg) {
        RequestVoteReply reply = new RequestVoteReply();
        //1.如果当前任期小于请求的任期，则更新当前任期
        if (arg.term < currentTerm) {
            reply.term = currentTerm;
            reply.voteGranted = false;
            return reply;
        }

        //2.如果请求任期大于当前任期，更新当前任期以及当前任期投票给的候选人状态
        if (arg.term > currentTerm) {
            currentTerm = arg.term;
            votedFor = -1; // 重置投票状态
            state = RoleState.FOLLOWER; // 转为跟随者状态

            // 重置选举超时时间，因为我们接收到了更高任期的请求
            resetElectionTimeout();

            System.out.println("Node " + selfId + " updated term to " + currentTerm + " and became FOLLOWER");
        }

        reply.term = currentTerm;

        boolean canVoted = votedFor == -1 || votedFor == arg.candidateId;
        boolean isLogUpToDate = (arg.lastLogTerm > this.getLastLogTerm()) ||
                (arg.lastLogTerm == this.getLastLogTerm() && arg.lastLogIndex >= this.getLastLogIndex());

        if (canVoted && isLogUpToDate) {
            votedFor = arg.candidateId;
            reply.voteGranted = true;

            System.out.println("Node " + selfId + " voted for " + arg.candidateId + " in term " + currentTerm);

            // 重置选举超时时间，因为我们刚刚投票给了一个候选人
            resetElectionTimeout();
        }
        else{
            reply.voteGranted = false;
            System.out.println(STR."Node \{selfId} denied vote for \{arg.candidateId} in term \{currentTerm} (canVoted=\{canVoted}, isLogUpToDate=\{isLogUpToDate})");
        }
        return reply;

    }

    public void becomeFollower(int term){
        state = RoleState.FOLLOWER;
        currentTerm = term;
        votedFor = -1; // 重置投票状态

        // 通知RaftNode状态变化
        if (nodeRef != null) {
            nodeRef.onBecomeFollower();
        }
    }


    public void becomeLeader(){
        state = RoleState.LEADER;

        for(int i = 0; i < peerIds.size(); i++){
            if (i < nextIndex.length) {
                nextIndex[i] = log.size(); // 初始化为当前日志长度（下一个要发送的索引）
            }
            if (i < matchIndex.length) {
                matchIndex[i] = 0; // 初始化每个从节点的匹配索引为0
            }
        }

        // 找到自己在peerIds中的索引
        int selfIndex = peerIds.indexOf(selfId);
        if (selfIndex >= 0 && selfIndex < matchIndex.length) {
            matchIndex[selfIndex] = getLastLogIndex(); // 自己的匹配索引为最后日志索引
        }

        System.out.println("Node " + selfId + " became LEADER for term " + currentTerm);

        // 通知RaftNode启动心跳定时器
        if (nodeRef != null) {
            nodeRef.onBecomeLeader();
        }

        // 立即发送一次心跳宣告Leader身份
        sendHeartbeats();
    }




    /**
     * 处理AppendEntries请求（心跳）
     * @param args 心跳请求参数
     * @return 心跳回复
     */
    public synchronized AppendEntriesReply handleAppendEntriesRequest(AppendEntriesArgs args) {
        AppendEntriesReply reply = new AppendEntriesReply();

        // 1. 如果leader的任期小于当前任期，拒绝请求
        if (args.term < currentTerm) {
            reply.term = currentTerm;
            reply.success = false;
            return reply;
        }

        if(args.term > currentTerm){
            System.out.println("任期更新: " + currentTerm + " -> " + args.term);
            currentTerm = args.term;
            state = RoleState.FOLLOWER; // 转为跟随者状态
            votedFor = -1; // 重置投票状态
            //todo 持久化
        }

        // 重置心跳和选举超时
        resetElectionTimeout();

        // 收到来自leader的有效AppendEntries，转为follower
        if (state == RoleState.CANDIDATE) {
            state = RoleState.FOLLOWER;
        }
        
        reply.term = currentTerm;

        //2.reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if(args.prevLogIndex >= log.size()){
            reply.success = false;
            reply.xLen = log.size();
            reply.xIndex = -1;
            reply.xTerm = -1; // 没有冲突的任期
            System.out.println("失败: prevLogIndex " + args.prevLogIndex + " 超出日志范围，当前日志长度为 " + log.size());
            return reply;
        }

        if(args.prevLogTerm != log.get(args.prevLogIndex).getLogTerm()){
            reply.success = false;
            int conflictTerm = log.get(args.prevLogIndex).getLogTerm();
            reply.xTerm = conflictTerm;

            reply.xIndex = args.prevLogIndex;
            while (reply.xIndex > 0 && log.get(reply.xIndex - 1).getLogTerm() == conflictTerm) {
                reply.xIndex--;
            }
            reply.xLen = log.size();

            System.out.println(STR."失败: prevLogTerm \{args.prevLogTerm} 与日志条目不匹配，当前日志长度为 \{log.size()}");
            return reply;
        }

        //3.If an existing entry conflicts with a new one (same index but different term), delete the existing entry and all that follow it
        //4.Append any new entries not already in the log
        if(!args.entries.isEmpty()){
            int insertIndex = args.prevLogIndex +1;
            System.out.println("接收到新日志条目，插入索引: " + insertIndex + ", 条目数量: " + args.entries.size());

            int conflictIndex = -1;
            for(int i=0;i<args.entries.size();i++){
                int currentIndex = insertIndex + i;
                if(currentIndex < log.size()){
                    if(log.get(currentIndex).getLogTerm() != args.entries.get(i).getLogTerm()){
                        conflictIndex = currentIndex;
                        break; // 找到冲突的索引，停止处理
                    }
                }else{
                    conflictIndex = currentIndex;
                    break;
                }
            }
            if(conflictIndex != -1){
                if(conflictIndex < log.size()){
                    List<LogEntry> subList = new ArrayList<>(log.subList(0, conflictIndex));
                    log.clear();
                    log.addAll(subList); // 保留冲突前的日志
                }

                if(lastApplied >= conflictIndex){
                    int oldLastApplied = lastApplied;
                    lastApplied = conflictIndex - 1; // 更新lastApplied为冲突索引前一个
                    System.out.println("更新 lastApplied: " + oldLastApplied + " -> " + lastApplied);
                }
                if(commitIndex >= conflictIndex){
                    int oldCommitIndex = commitIndex;
                    commitIndex = conflictIndex - 1; // 更新commitIndex为冲突索引前一个
                    System.out.println("更新 commitIndex: " + oldCommitIndex + " -> " + commitIndex);
                }

                int startAppendIndex = conflictIndex - insertIndex;
                for(int i=startAppendIndex; i<args.entries.size();i++){
                    log.add(args.entries.get(i));
                    System.out.println("追加日志条目: " + args.entries.get(i));
                }
                //todo 持久化日志
            }
        }
        else{
            if(log.size() > args.prevLogIndex +1){
                System.out.println("心跳截断多余日志，清除索引 " + (args.prevLogIndex + 1) + " 之后的日志");
                List<LogEntry> subList = new ArrayList<>(log.subList(0, args.prevLogIndex + 1));
                log.clear();
                log.addAll(subList);
                //todo 持久化日志

            }
        }

        //5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if(args.leaderCommit > commitIndex){
            int oldCommitIndex = commitIndex;
            int lastNewEntryIndex = args.prevLogIndex + args.entries.size();
            commitIndex = Math.min(args.leaderCommit, lastNewEntryIndex);
            System.out.println("更新 commitIndex: " + oldCommitIndex + " -> " + commitIndex);

        }
        reply.success = true;
        reply.xLen = log.size();
        reply.xIndex = -1; // 没有冲突的索引
        reply.xTerm = -1; // 没有冲突的任期

        return reply;

    }

    /**
     * 发送心跳到所有节点
     */
    public void sendHeartbeats() {
        AppendEntriesArgs args;
        if (state != RoleState.LEADER) {
            return;
        }


        for (Integer peerId : peerIds) {
            if (peerId == selfId) {
                continue;
            }

            synchronized (lock){
                if(state != RoleState.LEADER){
                    return;
                }

                int prevLogIndex = nextIndex[peerId] - 1; // 修复：prevLogIndex应该是nextIndex - 1
                List<LogEntry> entries;

                if(nextIndex[peerId] <= getLastLogIndex()){
                    int startArrayIndex = nextIndex[peerId];
                    if(startArrayIndex < log.size()) {
                        entries = new ArrayList<>(log.subList(startArrayIndex, log.size()));
                    }else{
                        entries = new ArrayList<>();
                    }
                }
                else{
                    entries = new ArrayList<>();
                }

                args = new AppendEntriesArgs();
                args.term = currentTerm;
                args.leaderId = selfId;
                args.prevLogIndex = prevLogIndex;
                args.prevLogTerm = log.get(prevLogIndex).getLogTerm();
                args.entries = entries;
                args.leaderCommit = commitIndex;
            }


            AppendEntriesArgs finalArgs = args;
            network.sendAppendEntries(peerId, args)
                    .thenAccept(reply -> {
                        synchronized (lock){
                            if (state!= RoleState.LEADER || currentTerm != finalArgs.term){
                                return;
                            }

                            if(reply.term > currentTerm){
                                System.out.println("心跳过程中发现更高任期，退位");
                                currentTerm = reply.term;
                                state = RoleState.FOLLOWER;
                                votedFor = -1;
                                resetElectionTimeout();
                                //todo 持久化
                            }
                            else if(reply.success){
                                nextIndex[peerId] = finalArgs.prevLogIndex + finalArgs.entries.size() +1;
                                matchIndex[peerId] = finalArgs.prevLogIndex + finalArgs.entries.size();

                                if(finalArgs.entries.size() >0){
                                    System.out.println("心跳中成功复制到节点 " + peerId + "，nextIndex: " + nextIndex[peerId] + ", matchIndex: " + matchIndex[peerId]);
                                    updateCommitIndex(); //检查是否可以提交新的日志条目
                                }
                            }
                            else{
                                if(reply.term < currentTerm){
                                    int oldNextIndex = nextIndex[peerId];
                                    nextIndex[peerId] = optimizeNextIndex(peerId, reply);
                                    System.out.println("心跳中节点 " + peerId + " 返回失败，更新 nextIndex: " + oldNextIndex + " -> " + nextIndex[peerId]);
                                }
                            }
                        }
                    })
                    .exceptionally(throwable -> {
                        System.err.println("Failed to send heartbeat to node " + peerId + ": " + throwable.getMessage());
                        return null;
                    });
        }
    }

    public void updateCommitIndex(){
        if(state != RoleState.LEADER){
            return;
        }

        for (int n=getLastLogIndex(); n > commitIndex; n--){
            if(n<log.size() && log.get(n).getLogTerm() == currentTerm){
                int count = 1; // 自己先投一票

                for(int i : peerIds){
                    if(i != selfId && matchIndex[i] >=n){
                        count++;
                    }
                }

                if(count >= peerIds.size()/2 +1){
                    System.out.println("提交日志条目到索引 " + n + "，当前任期: " + currentTerm);
                    commitIndex = n;
                    return;
                }else{
                    System.out.println("无法提交日志条目到索引 " + n + "，需要更多投票，当前投票数: " + count);
                }
            }
        }
    }





    public int optimizeNextIndex(int serverId, AppendEntriesReply reply){
        if(reply.xTerm == -1){
            return reply.xLen;
        }

        int conflictTerm = reply.xTerm;
        int lastIndexOfXTerm = -1;

        for(int i=log.size()-1;i>=0;i--){
            if(log.get(i).getLogTerm() == conflictTerm){
                lastIndexOfXTerm = i;
                break;
            }
        }

        if(lastIndexOfXTerm != -1){
            return lastIndexOfXTerm+1;
        }
        else{
            return reply.xIndex;
        }
    }



    public synchronized AppendResult start(String command){
        AppendResult result = new AppendResult();
        if(state != RoleState.LEADER){
            result.setCurrentTerm(currentTerm);
            result.setNewLogIndex(-1);
            result.setSuccess(false);
            return result;
        }

        final int curTerm = currentTerm;
        LogEntry newEntry = new LogEntry(log.size(), curTerm, command);

        log.add(newEntry);
        //todo 持久化日志

        if(state != RoleState.LEADER || currentTerm !=curTerm){
            log.removeLast();
            //todo 持久化回滚
            System.out.println("当前状态不是领导者或任期已变更，无法添加日志条目");
            result.setCurrentTerm(currentTerm);
            result.setNewLogIndex(-1);
            result.setSuccess(false);
            return result;
        }

        replicationLogEntries();

        result.setCurrentTerm(currentTerm);
        result.setNewLogIndex(newEntry.getLogIndex());
        result.setSuccess(true);
        return result;
    }

    public void replicationLogEntries(){
        synchronized (lock){
            if(state != RoleState.LEADER){
                return; // 只有领导者可以进行日志复制
            }
        }

        for(Integer peerId: peerIds){
            if( peerId == selfId){
                continue;
            }
            Thread.ofVirtual().start(() -> {
                sendAppendEntriesToPeer(peerId);
            });
        }
    }

    private void sendAppendEntriesToPeer(Integer peerId) {
        AppendEntriesArgs args;
        final int curTerm;
        synchronized (lock){
            if(state != RoleState.LEADER){
                return; // 只有领导者可以进行日志复制
            }

            int prevLogIndex = nextIndex[peerId]-1;
            List<LogEntry> entries;

            if(nextIndex[peerId] <= getLastLogIndex()){
                int startArrayIndex = nextIndex[peerId];
                if(startArrayIndex <log.size()){
                    entries = new ArrayList<>(log.subList(startArrayIndex, log.size()));
                }else{
                    entries = new ArrayList<>();
                }
            }else{
                entries = new ArrayList<>();
            }

            args = new AppendEntriesArgs().builder().term(currentTerm)
                    .leaderId(selfId)
                    .prevLogIndex(prevLogIndex)
                    .prevLogTerm(log.get(prevLogIndex).getLogTerm())
                    .entries(entries)
                    .leaderCommit(commitIndex).build();
            curTerm = currentTerm;
        }

        network.sendAppendEntries(peerId, args).thenAccept(
                reply ->{
                    synchronized (lock){
                        if(state !=RoleState.LEADER || currentTerm != curTerm){
                            return;
                        }

                        if(reply.success){
                            nextIndex[peerId] = args.prevLogIndex+ args.entries.size() + 1;
                            matchIndex[peerId] = args.prevLogIndex + args.entries.size();
                            System.out.println("节点 " + selfId + " 成功复制日志到节点 " + peerId + "，nextIndex: " + nextIndex[peerId] + ", matchIndex: " + matchIndex[peerId]);
                            updateCommitIndex();
                        }
                        else{
                            if(reply.term > curTerm){
                                System.out.println("发现更高任期，退位");
                                currentTerm = reply.term;
                                state = RoleState.FOLLOWER;
                                votedFor = -1; // 重置投票状态
                                resetElectionTimeout();
                            }
                            else{
                                int oldNextIndex = nextIndex[peerId];
                                nextIndex[peerId] = optimizeNextIndex(peerId, reply);
                                System.out.println("节点 " + selfId + " 复制日志到节点 " + peerId + " 失败，更新 nextIndex: " + oldNextIndex + " -> " + nextIndex[peerId]);
                            }
                        }
                    }
                }
        ).exceptionally(throwable -> {
            System.err.println("Failed to send AppendEntries to node " + peerId + ": " + throwable.getMessage());
            return null;
        });
    }

    // ======================== 公共接口方法 ========================
    
    /**
     * 获取当前提交索引
     */
    public int getCommitIndex() {
        return commitIndex;
    }
    
    /**
     * 获取当前任期
     */
    public int getCurrentTerm() {
        return currentTerm;
    }
    
    /**
     * 获取当前状态
     */
    public RoleState getState() {
        return state;
    }
    
    /**
     * 获取投票对象
     */
    public int getVotedFor() {
        return votedFor;
    }
    
    /**
     * 获取日志大小
     */
    public int getLogSize() {
        return log.size();
    }
    
    /**
     * 获取最后一条日志的索引
     */
    public int getLastLogIndex() {
        return log.size() - 1; // 日志索引从0开始，但第0个是dummy entry
    }
    
    /**
     * 获取最后一条日志的任期
     */
    public int getLastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).getLogTerm();
    }
    
    /**
     * 获取指定索引的日志条目
     */
    public LogEntry getLogEntry(int index) {
        if (index <= 0 || index > log.size()) {
            return null;
        }
        return log.get(index - 1);
    }
    
    /**
     * 检查是否为Leader
     */
    public boolean isLeader() {
        return state == RoleState.LEADER;
    }
}
