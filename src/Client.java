import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;

public class Client {
    public static void main(String[] args) {
        // 最多允许（32*4*机器个数)个并发请求。如果太多的并发可能会发生获取连接失败。
        PoolingOptions poolingOptions = new PoolingOptions();
        // 每个连接允许32请求并发。
        poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, 32);
        // 这就完成了一个对连接池的配置。表示和集群里的机器至少有2个连接。注意是和集群里的每个机器都至少有2个连接。
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 2);
        // 最多有4个连接
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 4);
        Cluster cluster = Cluster.builder().addContactPoints("192.168.1.101").withCredentials(username, password)
                .withPoolingOptions(poolingOptions);
        // 配置完之后，获取session，就可以全程序单例使用了。
    }

    public void remote() {
        // 以上是说的集群部署在一个机房，只有一个数据中心DC的情况，如果有多个数据中心。要设置REMOTE连接数。
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, 32);
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 2);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 4);
        poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, 2);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, 4);
    }
}
