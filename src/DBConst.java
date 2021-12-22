public final class DBConst {
	
	public static final String DBConnectionStr ="jdbc:mariadb://192.168.102.110:3306/HCLOUD";
	public static final String userName = "root";
	public static final String userPwd = "P@$$w0rd";
	public static final String driver = "org.mariadb.jdbc.Driver";
	public static final int policyServerPort = 10001;
	public static final int testPolicyServerPort = 8888;
	//public static final int PolicyReceiverPort = 10001;
	public static final int PolicyReceiverPort = 30001;
	public static final int NetworkLatencyPort = 30002;
	public static final int testNetworkLatencyPort = 30002;
	public static final String testNewrorkLatencyServer = "192.168.102.190";
	public static final String NetworkLatencyServer= "192.168.102.190";
	public static final String mode ="REAL";
    //public static final String mode ="TEST";
	//public static final String testVM ="localhost";
	public static final String testVM ="192.168.101.200";
	
}
