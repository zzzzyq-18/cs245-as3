package cs245.as3.tool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;


public class LogData {

  //日志类型
  private int type;
  //事件ID
  private long txID;
  //键
  private long key;
  //值
  private byte[] value;
  //上一条日志的偏移量
  private int preOffset;
  //记录创建了但未提交的事件ID
  private ArrayList<Long> activeTxns;
  //创建但未提交的事件ID对应的最早偏移量
  private int activeTxnStartEarlistOffset; // 4字节
  //日志大小
  private int size;
  //偏移量
  private volatile int offset;

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public long getTxID() {
    return txID;
  }

  public void setTxID(long txID) {
    this.txID = txID;
  }

  public long getKey() {
    return key;
  }

  public void setKey(long key) {
    this.key = key;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public ArrayList<Long> getActiveTxns() {
    return activeTxns;
  }

  public void setActiveTxns(ArrayList<Long> activeTxns) {
    this.activeTxns = activeTxns;
  }

  public int getActiveTxnStartEarlistOffset() {
    return activeTxnStartEarlistOffset;
  }

  public void setActiveTxnStartEarlistOffset(int activeTxnStartEarlistOffset) {
    this.activeTxnStartEarlistOffset = activeTxnStartEarlistOffset;
  }

  public int getPreOffset() {
    return preOffset;
  }

  public void setPreOffset(int preOffset) {
    this.preOffset = preOffset;
  }

  public byte[] getByteArray(ByteArrayOutputStream arrayOutputStream) {
    arrayOutputStream.write(type);  // 主要是type取值范围比较小可以用1字节
    switch (type) {
      case LogType.Txn_Create:
        size = 17; // type + txID + preOffset + size;
        byte[] b = Typechange.longToByte(txID);
        arrayOutputStream.write(b, 0, b.length);
        break;
      case LogType.OPERATION:
        size = 25 + value.length; // type + txID + key + value + preOffset + size;
        b = Typechange.longToByte(txID);
        arrayOutputStream.write(b, 0, b.length);
        b = Typechange.longToByte(key);
        arrayOutputStream.write(b, 0, b.length);
        arrayOutputStream.write(value, 0, value.length);
        break;
      case LogType.COMMIT_TXN:
        size = 17; // type + txID + preOffset + size;
        b = Typechange.longToByte(txID);
        arrayOutputStream.write(b, 0, b.length);
        break;
      case LogType.START_check:
        size = activeTxns.size() * 8 + 13; // type + activeTSize * 8 + activeTxnStartEarlistOffset + preOffset + size;
//        b = BytesUtils.intToByte(activeTxns.size());
//        arrayOutputStream.write(b, 0, b.length);
        for (long txnId : activeTxns) {
          b = Typechange.longToByte(txnId);
          arrayOutputStream.write(b, 0, b.length);
        }
        b = Typechange.intToByte(activeTxnStartEarlistOffset);
        arrayOutputStream.write(b, 0, b.length);
        break;
      case LogType.END_check:
        size = 9; // type + preOffset + size; 1 + 4 + 4
        break;
      default:
        // abort
        size = 17; // type + txnId + preOffset + size 1 + 8 + 4 + 4
        b = Typechange.longToByte(txID);
        arrayOutputStream.write(b, 0, b.length);
    }

    byte[] b = Typechange.intToByte(preOffset);
    arrayOutputStream.write(b, 0, b.length);
    b = Typechange.intToByte(size);
    arrayOutputStream.write(b, 0, b.length);

    // flush
    try {
      arrayOutputStream.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
    byte[] ans = arrayOutputStream.toByteArray();
    arrayOutputStream.reset();
    return ans;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LogData logData = (LogData) o;
    return type == logData.type &&
        txID == logData.txID &&
        key == logData.key &&
        activeTxnStartEarlistOffset == logData.activeTxnStartEarlistOffset &&
        size == logData.size &&
        preOffset == logData.preOffset &&
        Arrays.equals(value, logData.value) &&
        Objects.equals(activeTxns, logData.activeTxns);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(type, txID, key, activeTxns, activeTxnStartEarlistOffset, preOffset, size);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }
}
