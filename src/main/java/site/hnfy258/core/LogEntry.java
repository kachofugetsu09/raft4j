package site.hnfy258.core;

import lombok.*;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class LogEntry implements Serializable {
    private int logIndex;
    private int logTerm;
    private String command; // 简化版本，使用字符串作为命令

    public int getLogTerm(int index){
        if(index <0){
            return -1;
        }
        if(index ==0){
            return 0;
        }
        return logTerm;
    }
}
