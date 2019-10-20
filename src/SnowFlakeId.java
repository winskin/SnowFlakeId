import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 0 - 0000000000 0000000000 0000000000 0000000000 0 - 00000 - 00000 - 000000000000
 *
 * 1位标识，由于long基本类型在Java中是带符号的，最高位是符号位，正数是0，负数是1，所以id一般是正数，最高位是0
 * 41位时间截(毫秒级)，注意，41位时间截不是存储当前时间的时间截，而是存储时间截的差值（当前时间截 - 开始时间截)，
 *      开始时间截一般是我们的id生成器开始使用的时间，由我们程序来指定的。
 *      41位的时间截，可以使用69年，年T = (1L << 41) / (1000L * 60 * 60 * 24 * 365) = 69
 * 10位的数据机器位，可以部署在1024个节点，包括5位datacenterId和5位workerId
 * 12位序列，毫秒内的计数，12位的计数顺序号支持每个节点每毫秒(同一机器，同一时间截)产生4096个ID序号
 *
 * 加起来刚好64位，为一个Long型。(转换成字符串后长度最多19)
 * SnowFlake的优点是，整体上按照时间自增排序，并且整个分布式系统内不会产生ID碰撞(由datacenter数据中心ID和workerId机器ID作区分)，并且效率较高，
 * 经测试，SnowFlake每秒能够产生26万ID左右。
 *
 * 优点：
 * 1.毫秒数在高位，自增序列在低位，整个ID都是趋势递增的。
 * 2.不依赖数据库等第三方系统，以服务的方式部署，稳定性更高，生成ID的性能也是非常高的。
 * 3.可以根据自身业务特性分配bit位，非常灵活。
 *
 * 缺点：
 * 强依赖机器时钟，如果机器上时钟回拨，会导致发号重复或者服务会处于不可用状态。
 *
 * @author Administrator
 */
public class SnowFlakeId {

    /**
     * 起始的时间戳
     */
    private final static long START_STAMP = 1571533232000L;

    /**
     * 每一部分占用的位数
     */
    private final static long SEQUENCE_BIT = 12; //序列号占用的位数
    private final static long MACHINE_BIT = 5;   //机器标识占用的位数
    private final static long DATACENTER_BIT = 5;//数据中心占用的位数

    /**
     * 每一部分的最大值
     * 计算原理：
     * 1向左移动12位数(结果是13位)：1<<12 = 1000000000000
     * 只需要13位数-1即12位的最大值，这里使用它的反码（1000000000000的反码 == 0111111111111）
     * ~ 表示该数的反码
     */
    private final static long MAX_DATACENTER_NUM = ~(-1L << DATACENTER_BIT);    // 31
    private final static long MAX_MACHINE_NUM = ~(-1L << MACHINE_BIT);          // 31
    private final static long MAX_SEQUENCE = ~(-1L << SEQUENCE_BIT);            // 4095

    /**
     * 每一部分向左的位移
     */
    private final static long MACHINE_LEFT = SEQUENCE_BIT;
    private final static long DATACENTER_LEFT = SEQUENCE_BIT + MACHINE_BIT;
    private final static long TIMESTAMP_LEFT = DATACENTER_LEFT + DATACENTER_BIT;

    private long datacenterId;  //数据中心
    private long machineId;     //机器标识
    private long sequence = 0L; //序列号
    private long lastStamp = -1L;//上一次时间戳

    /**
     * 实例化
     * @param datacenterId  数据中心id
     * @param machineId     机器标识id
     */
    public SnowFlakeId(long datacenterId, long machineId) {
        if (datacenterId > MAX_DATACENTER_NUM || datacenterId < 0) {
            throw new IllegalArgumentException("datacenterId 不能大于 MAX_DATACENTER_NUM 或小于 0");
        }
        if (machineId > MAX_MACHINE_NUM || machineId < 0) {
            throw new IllegalArgumentException("machineId 不能大于 MAX_MACHINE_NUM 或小于 0");
        }
        this.datacenterId = datacenterId;
        this.machineId = machineId;
    }

    /**
     * 产生下一个ID
     *
     * @return
     */
    public synchronized long nextId() {

        // 当前时间戳
        long currStamp = getNewStamp();
        if (currStamp < lastStamp) {
            throw new RuntimeException("时钟回拨,拒绝生成ID");

        } else if (currStamp == lastStamp) {
            //相同毫秒内，序列号自增
            sequence = (sequence + 1) & MAX_SEQUENCE;
            //同一毫秒的序列数已经达到最大
            if (sequence == 0L) {
                currStamp = getNextMill();
            }

        } else {
            //不同毫秒内，序列号置为0
            sequence = 0L;
        }

        lastStamp = currStamp;

        return (currStamp - START_STAMP) << TIMESTAMP_LEFT // 时间戳部分
                | datacenterId << DATACENTER_LEFT       // 数据中心部分
                | machineId << MACHINE_LEFT             // 机器标识部分
                | sequence;                             // 序列号部分
    }

    /**
     * 获取下一个最新的时间戳
     * 如果不是最新会一直循环到最新
     * @return
     */
    private long getNextMill() {
        long mill = getNewStamp();
        while (mill <= lastStamp) {
            mill = getNewStamp();
        }
        return mill;
    }

    /**
     * 获得当前时间戳
     * @return
     */
    private long getNewStamp() {
        return System.currentTimeMillis();
    }


    /**
     * main
     * @param args
     */
    public static void main(String[] args) {

        /*
        0 - 0000000000 0000000000 0000000000 0000000000 0 - 00000 - 00000 - 000000000000
         */

        long start = System.currentTimeMillis();
        SnowFlakeId snowFlake = new SnowFlakeId(2, 3);
        Map<Long, Integer> longs = new HashMap<>();
        ExecutorService executorService = Executors.newFixedThreadPool(500);
        for (int i = 0; i < 1000000; i++) {
            executorService.execute(() -> {
                Long tmp = snowFlake.nextId();

                if (!longs.containsKey(tmp)) {
                    longs.put(tmp, 0);
                    System.out.println("生成新的：" + tmp + "二进制：" + Long.toBinaryString(tmp));
                } else {
                    int count = longs.get(tmp) + 1;
                    longs.put(tmp, count);
                    System.out.println("重复：" + tmp + " 次数：" + longs.get(tmp));
                }
            });
        }
        System.out.println("用时毫秒" + (System.currentTimeMillis() - start));

    }

}
