package cs245.as3.tool;


public class LogType {

    //事件创建
    public final static int Txn_Create = 0;

    //数据操作
    public final static int OPERATION = 1;

    //事件提交
    public final static int COMMIT_TXN = 2;

    //检查点起点
    public final static int START_check = 3;

    //检查点终点
    public final static int END_check = 4;

}
