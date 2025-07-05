package site.hnfy258;

import lombok.Getter;
import lombok.Setter;
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
    private final int heartbeatInterval = 50; // 心跳间隔 50ms

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
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.electionTimeout = generateElectionTimeout();
        this.voteCount = 0;
    }

    private int generateElectionTimeout() {
        // 选举超时时间为 150-300ms 之间的随机值
        return ThreadLocalRandom.current().nextInt(150, 300);
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
            System.out.println("Node " + selfId + " denied vote for " + arg.candidateId + " in term " + currentTerm + 
                             " (canVoted=" + canVoted + ", isLogUpToDate=" + isLogUpToDate + ")");
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
                nextIndex[i] = log.size(); // 初始化每个从节点的下一个索引为当前日志长度
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

    private int getLastLogIndex() {
        if (log.isEmpty()) {
            return 0; // 如果日志为空，返回0
        }
        return log.size() - 1; // 返回最后一个日志条目的索引
    }

    private int getLastLogTerm() {
        if (log.isEmpty()) {
            return -1; // 如果日志为空，返回-1
        }
        return log.get(log.size() - 1).getLogTerm(); // 返回最后一个日志条目的任期
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
        
        // 2. 如果收到来自新leader的更高任期，更新状态
        if (args.term > currentTerm) {
            becomeFollower(args.term);
        } else if (state != RoleState.FOLLOWER) {
            // 即使任期相同，如果不是follower也要转为follower
            state = RoleState.FOLLOWER;
            if (nodeRef != null) {
                nodeRef.onBecomeFollower();
            }
        }
        
        // 3. 重置选举超时
        resetElectionTimeout();
        
        reply.term = currentTerm;
        reply.success = true;

        
        return reply;
    }

    /**
     * 发送心跳到所有节点
     */
    public void sendHeartbeats() {
        if (state != RoleState.LEADER) {
            return;
        }

        
        for (Integer peerId : peerIds) {
            if (peerId == selfId) {
                continue;
            }
            
            // 创建心跳请求
            AppendEntriesArgs args = new AppendEntriesArgs();
            args.term = currentTerm;
            args.leaderId = selfId;
            args.prevLogIndex = 0; // 简化版本，不处理日志
            args.prevLogTerm = 0;
            args.entries = new ArrayList<>(); // 空心跳
            args.leaderCommit = commitIndex;
            
            // 异步发送心跳
            network.sendAppendEntries(peerId, args)
                .thenAccept(reply -> handleAppendEntriesReply(peerId, args.term, reply))
                .exceptionally(throwable -> {
                    System.err.println("Failed to send heartbeat to node " + peerId + ": " + throwable.getMessage());
                    return null;
                });
        }
    }

    /**
     * 处理心跳回复
     * @param serverId 服务器ID
     * @param requestTerm 请求的任期
     * @param reply 心跳回复
     */
    private synchronized void handleAppendEntriesReply(int serverId, int requestTerm, AppendEntriesReply reply) {
        if (reply == null) {
            return;
        }
        
        // 如果收到更高任期的回复，转为follower
        if (reply.term > currentTerm) {
            becomeFollower(reply.term);
        }
        
        // 如果任期不匹配，忽略回复
        if (currentTerm != requestTerm || state != RoleState.LEADER) {
            return;
        }
        
        // 心跳成功，在实际实现中这里会更新nextIndex和matchIndex
        if (reply.success) {
            // 心跳成功，节点是活跃的
        }
    }


}
