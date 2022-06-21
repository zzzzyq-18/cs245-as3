package cs245.as3;

import cs245.as3.interfaces.*;
import cs245.as3.interfaces.StorageManager.TaggedValue;
import cs245.as3.tool.LogType;
import cs245.as3.tool.Typechange;
import cs245.as3.tool.LogData;


import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

public class TransactionManager {
	private HashMap<Long, TaggedValue> latestValues;
	//记录的是每个key值的情况，包括偏移量和value
	private HashMap<Long, HashMap<Long, byte[]>> writesets;
	//记录需要写入的数据，第一个long是事件ID，第二个是map分别是key和value
	private HashSet<Long> datawriteTxns;
	//活跃的事件ID集合，还未提交
	private HashSet<Long> commitTxns;
	//提交但未持久化的事件ID集合
	private HashSet<Long> persistTxns;
	//已经持久化的事件ID集合
	private ArrayList<Long> commitTxnsInCkpt;
	//做检查点时已经提交但是没有持久化的事件ID集合
	private ByteArrayOutputStream bArray;
	//输出流
	private int preLogSize;
	//上一次的日志大小
	private boolean isincheck;
	//标识是否处于检查状态
	private HashMap<Long, Integer> earlistTxnscreat;
	//每个事务可以有多次操作，记录每个事务，最早的日志记录位置，不在意操作哪个key
	private HashMap<Long, HashMap<Long, Integer>> txnslastoper;
	//记录每个事件对每个key的最后一次操作的偏移量
	private int trunctionPos;
	//截断的位移，相当于这之前的一定无需重做
	private int preLogPos;
	//上一次的日志位置
	private StorageManager s_manager;
	//存储管理
	private LogManager l_manager;
	//日志管理
	public TransactionManager() {
		writesets = new HashMap<>();
		latestValues = null;//记录对应key的操作
		bArray = new ByteArrayOutputStream();//是一个输出流
		datawriteTxns = new HashSet<>();//当前正在活跃的事务ID合集
		earlistTxnscreat = new HashMap<>();
		txnslastoper = new HashMap<>();//每个事务中所操作key的最后一条偏移
		commitTxns = new HashSet<>();//记录着所有已经提交的事件ID
		persistTxns = new HashSet<>();
		commitTxnsInCkpt = new ArrayList<>();
		preLogSize = 0;//初始化日志大小为0
		isincheck = false;
		preLogPos = -1; //初始化日志指针的偏移量
	}
	//初始化和恢复函数
	public void initAndRecover(StorageManager storageManager, LogManager logManager) {
		//sm中的存储是以key为单位的，不在意时间ID，找到key之后，用一个结构体存储了最近版本，persist版本，和历史记录。
		//然后在下面函数调用中，记录每个Key的persist版本，返回的latestvalues是HashMap<Long, TaggedValue>，long为key
		latestValues = storageManager.readStoredTable();//这时候获取的是持久化的版本，key和其tag，value

		s_manager = storageManager;
		l_manager = logManager;

		// 获取上一条日志记录位置偏移,也就是返回日志的末端
		int curPos = l_manager.getLogEndOffset();
		if (curPos - 4 >= l_manager.getLogTruncationOffset()) {
			//只要末端距离截断的偏移量大于4，就取最后4个字节
			byte[] b = l_manager.readLogRecord(curPos - 4, 4);
			//不论什么类型的日志记录，其最后4个字节都表示该条日志记录的大小。
			preLogPos = curPos - Typechange.byteToInt(b);
			//获取到最后一条记录的大小后，上式计算相当于更新preLogPos为最后一条日志的起始地方。
		}

		if (preLogPos == -1) return;

		// 开始从后往前扫描日志
		boolean isEndCkpt = false;	// 用于判断从后往前是否遇到结束检查点的日志
		int pos = preLogPos;
		int limitPos = l_manager.getLogTruncationOffset();		// 日志当前截断点
		HashSet<Long> needRedoTxns = new HashSet<>();	// 存储需要重做的事务ID
		ArrayList<LogData> logDatas = new ArrayList<>();	// 需要重做的日志记录
		int checkLimitPos = -1;	// 日志恢复需要检查的最小位置，有的话就是从后往前扫描第一个与endCkpt记录匹配的startCkpt记录中活跃事务最早start的偏移
		//while限制表示截断点之前的不用再考虑了
		while (pos >= limitPos) {
			//第二个限制是日志恢复的最小位置，初始化为-1，后面会更新
			if (pos < checkLimitPos) break;

			//获取完整的最后一条日志记录
			byte[] b = l_manager.readLogRecord(pos, curPos - pos);
			//转化为类对象
			LogData logData = Typechange.byteToLogRecord(b);
			//核心是找到需要重做的事件ID
			switch (logData.getType()) {
				case LogType.OPERATION:
					//如果是需要重做的事件对应的操作，那一定是要重做的日志操作
					if (needRedoTxns.contains(logData.getTxID())) {
						logData.setOffset(pos);
						logDatas.add(logData);
					}
					break;
				case LogType.COMMIT_TXN:
					//在checkLimitPos没有修改之前的所有提交记录，都认为是仅提交，没有持久化的数据，所有都需要重做
					if (checkLimitPos == -1) needRedoTxns.add(logData.getTxID());
					break;
				case LogType.START_check:
					//如果没有经过end，只有start，则是不会更新checkLimitPos的。
					//只有找到了一组配对的start和end,就更新checkLimitPos为start时还没提交的事件对应的最早的偏移量。
					//且只更新这一次
					if (isEndCkpt && checkLimitPos == -1) {
						checkLimitPos = logData.getActiveTxnStartEarlistOffset();
					}
					break;
				case LogType.END_check:
					//如果从后往前扫描先碰到的是END，说明start,end是成对出现的。
					//end意味着其对应的最近start，那个时候的提交列表已经全部持久化。
					isEndCkpt = true;
					break;
				default:

			}
			curPos = pos;//这条日志记录处理了之后，换成下一个数据
			pos = logData.getPreOffset();
		}

		// 开始重做，因为前面的添加顺序是从后往前，所以这里应该从前往后之星
		for (int i = logDatas.size() - 1; i >= 0; i --) {
			LogData logData = logDatas.get(i);
			//记录到sm中
			s_manager.queueWrite(logData.getKey(), logData.getOffset(), logData.getValue());
			//该类的变量，用于记录key对应的版本
			latestValues.put(logData.getKey(), new TaggedValue(logData.getOffset(), logData.getValue()));
		}
	}

	// 建立事件，需要完成的事情有
	// 1. 对于日志来说属于Txns_create类型，需要写入日志，包括事件类型，ID,上一条日志记录的偏移量，
	// 2. 记录为当前活跃，并在该创建的过程设置下其在日志记录中最早出现的位置
	public void start(long txID) {
		// 先写日志
		LogData logData = new LogData();
		logData.setType(LogType.Txn_Create);
		logData.setTxID(txID);
		logData.setPreOffset(preLogPos);
		//此处为-1
		//将outStream类型的bArray转换为字节流后写入到日志中,并且返回写入之前的日志位置
		preLogPos = l_manager.appendLogRecord(logData.getByteArray(bArray));
		//此处为0
	}

	//读取操作：latestValues的格式为HashMap<Long, TaggedValue>
	//返回当前参数key的情况
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	//记录需要写入的数据，执行了一个检查点。ps:没有实际操作，不写入日志
	public void write(long txID, long key, byte[] value) {
		//创建ID后，只要没有提交都放在activateTxns中
		datawriteTxns.add(txID);
		//记录ID和其在日志中最早出现的偏移,创建时的位置是最靠前的，所以此处直接就可以记录在earlistTxnscreat中
		earlistTxnscreat.put(txID, preLogPos);

		//先找到该ID对应的需要写入的map
		HashMap<Long, byte[]> writeset = writesets.getOrDefault(txID, new HashMap<>());
		//增添一条
		writeset.put(key, value);
		//然后更新所有的记录，因为txID如果重复会自动覆盖
		writesets.put(txID, writeset);
		doCheckPoint();
	}

	//检查本身也是一个事件，实际操作会写入日志
	//记录的内容包括活跃的ID，活跃的ID最早的日志偏移量
	private void doCheckPoint() {
		// 做检查点
		int logSize = l_manager.getLogEndOffset();//表示当前log的大小

		// 当前并没有正处于检查点中 且 日志大小不为空 且 最后一条日志大于300
		if (!isincheck && preLogSize != 0 && logSize - preLogSize > 300) {
			preLogSize = logSize;
			// 添加检查点日志记录
			LogData logData = new LogData();
			ArrayList<Long> txns = new ArrayList<>();
			int earlistLogSize = logSize + 1;

			//本质是遍历每一个还没提交的事件ID，遍历的结果是记录了 当前所有没提交的事件中，最早的那个事件偏移量
			for (Long txnId : datawriteTxns) {
				earlistLogSize = Math.min(earlistLogSize, earlistTxnscreat.get(txnId));
				txns.add(txnId);
			}
			//其余已经提交过的事件不管了（在别的地方会有记录），所以从earlistLogSize截断。
			if (earlistLogSize != logSize + 1)
				trunctionPos = earlistLogSize;
			logData.setActiveTxns(txns);
			logData.setActiveTxnStartEarlistOffset(earlistLogSize);
			logData.setType(LogType.START_check);
			logData.setPreOffset(preLogPos);
			preLogPos = l_manager.appendLogRecord(logData.getByteArray(bArray));
			isincheck = true;

			commitTxnsInCkpt.clear();	// 清除之前的
			for (long txnId : commitTxns) {
				commitTxnsInCkpt.add(txnId);
			}
			//commitTxns表示已经提交但没持久化的事件ID
			//commitTxnsInCkpt加了一个限制条件，在检查点时
		}
		//更新日志大小
		if (preLogSize == 0) {
			preLogSize = logSize;
		}
	}

	//前面的write是不用写入日志的，commit才需要。
	//每一条commit的数据都需要记录，且类型为operation
	public void commit(long txID) {
		datawriteTxns.remove(txID);	// 已提交，则从当前活跃事务集合中删掉
		commitTxns.add(txID);			// 添加到已提交集合中

		HashMap<Long, byte[]> writeset = writesets.get(txID);
		ArrayList<LogData> logDatas = new ArrayList<>();
		//不为空表示这个ID有需要提交的东西
		if (writeset != null) {
			for (Entry<Long, byte[]> x: writeset.entrySet()) {
				// 添加日志
				LogData logData = new LogData();
				logData.setTxID(txID);
				logData.setType(LogType.OPERATION);
				logData.setKey(x.getKey());
				logData.setValue(x.getValue());
				logData.setPreOffset(preLogPos);
				preLogPos = l_manager.appendLogRecord(logData.getByteArray(bArray));

				//更新一下变量，当前修改的是哪个key，就更新key最后被修改的值和偏移量
				latestValues.put(x.getKey(), new TaggedValue(preLogPos, x.getValue()));

				//txnsOffsetMap：HashMap<Long, HashMap<Long, Integer> >用于记录每个ID操作的每个key的最后一个偏移量
				HashMap<Long, Integer> txn = txnslastoper.getOrDefault(txID, new HashMap<>());
				txn.put(x.getKey(), preLogPos);
				txnslastoper.put(txID, txn);
				logData.setOffset(preLogPos);
				//这以上都是记录一条日志的设置

				//因为一个事件ID可以有多个提交，所以会有多条记录
				logDatas.add(logData);
			}
			//都已经记录到日志当中了，相当于提交了，就删除
			writesets.remove(txID);
		}
		// 添加日志
		LogData logData = new LogData();
		logData.setTxID(txID);
		logData.setType(LogType.COMMIT_TXN);
		logData.setPreOffset(preLogPos);
		preLogPos = l_manager.appendLogRecord(logData.getByteArray(bArray));

		// 先写日志再写数据到存储器sm
		for (LogData logData1 : logDatas) {
			s_manager.queueWrite(logData1.getKey(), logData1.getOffset(), logData1.getValue());
			//参数分别表示着存储的key，以及它的偏移量和具体值
		}
	}

	//清空所有需要写入的数据，和正在活跃的事件ID。 模拟故障
	public void abort(long txID) {
		writesets.remove(txID);
		datawriteTxns.remove(txID);
	}

	//数据持久化的过程，判断参数key这个数据是否被持久化，persisted_tag记录了当前持久化步骤进行到的位置
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
		doCheckPoint();
		ArrayList<Long> txns = new ArrayList<>();

		//依次遍历每一个已经提交的事件ID
		for (Long txnId : commitTxns) {
			if (txnslastoper.containsKey(txnId)) {
				//如果提交的事件ID有对该key进行过修改，如果它修改的偏移量在持久化进程的之前，表示已经持久化过了,不用再持久化了
				//补充 ：如果事件ID，对某个key的最终修改已经进行了持久化，也就不用再保持着对该key修改的记录了
				if (txnslastoper.get(txnId).containsKey(key)
						&& txnslastoper.get(txnId).get(key) <= persisted_tag) {
					txnslastoper.get(txnId).remove(key);
				}
				//如果某个事件ID没有需要持久化的数据了，则添加到persistTxns中。
				if (txnslastoper.get(txnId).size() == 0) {
					txns.add(txnId);
					persistTxns.add(txnId);//其中的事件ID，所有内容都持久化了

				}
			}
		}

		for (Long txnId : txns) {
			commitTxns.remove(txnId);
		}
		//此时commitTxns剩下的事件ID就是还需要进行持久化的事件ID

		//前提是当前处于检查点中，且满足结束检查点的要求，那就添加结束的日志记录
		if (isincheck && canEndCkpt()) {
			// 添加检查点结束日志记录
			LogData logData = new LogData();
			logData.setType(LogType.END_check);
			logData.setPreOffset(preLogPos);
			preLogPos = l_manager.appendLogRecord(logData.getByteArray(bArray));
			l_manager.setLogTruncationOffset(trunctionPos);	// 截断用于快速恢复
			isincheck = false;
		}
	}

	// 判断能否结束检查点操作。true表示可以结束
	// 如果最近一次开始检查，那时的提交列表中所有的事件ID都已经持久化了，那检查点就可以结束了
	private boolean canEndCkpt() {
		for (long txnId : commitTxnsInCkpt) {
			if (!persistTxns.contains(txnId)) {
				return false;
			}
		}
		//如果每一个事件ID都已经持久化了
		return true;
	}
}
