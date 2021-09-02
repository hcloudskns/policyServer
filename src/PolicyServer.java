import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class PolicyServer {
	public static void main(String args[]) {
		ServerSocket serverSocket = null;
		Socket socket = null;
		String message = "";
		
		Map<String, Client> clients = new HashMap<String, Client> ();
		
		try {
				if(DBConst.mode.equals("TEST")) {
					serverSocket = new ServerSocket(DBConst.testPolicyServerPort);
					System.out.println("테스트용 Policy 서버가 준비되었습니다.");
				}else {
					serverSocket = new ServerSocket(DBConst.policyServerPort);
					System.out.println("실제 Policy 서버가 준비되었습니다.");
				}
				
				while(true){
					
					socket = serverSocket.accept();
					
					Client c = new Client(socket.getInetAddress().toString(), Integer.toString(socket.getPort()),socket);
					clients.put(socket.getInetAddress().toString()+Integer.toString(socket.getPort()) , c);
					//System.out.println("IP: " + socket.getInetAddress().toString()+ " , " + Integer.toString(socket.getPort())) ;
	
					ServerReceiver receiver = new ServerReceiver(socket.getInetAddress().toString()+Integer.toString(socket.getPort()),clients );
					//ClientSender sender = new ClientSender(socket, message);
					//sender.start();
					receiver.start();
				}			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	} // main
		
} // class


class ServerReceiver extends Thread {
	Socket socket;
	DataInputStream in;
	
	ServerReceiver(String key,Map<String, Client> clients) {
		
		Client c = clients.get(key);
		this.socket = c.getSocket();
		try {
			in = new DataInputStream(socket.getInputStream());
		} catch(IOException e) {}

	}
	
	public void insertLog(String deviceType, String uuid, String deviceName, String logMessage) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		String strSQL ="";
		
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal1 = Calendar.getInstance();
		String strDate = sdf1.format(cal1.getTime());
		
		int r =0;

		try
		{
			Class.forName("org.mariadb.jdbc.Driver");
			conn = DriverManager.getConnection(DBConst.DBConnectionStr,DBConst.userName, DBConst.userPwd);
						
			strSQL = "insert into TB_TR_LOG(DEVICE_TYPE, UUID, DEVICE_NAME,LOG_MESSAGE,LOG_DATE) values(?, ?, ?, ?, ?)";
			pstmt = conn.prepareStatement(strSQL);
			
			pstmt.setString(1,deviceType );
			pstmt.setString(2, uuid);
			pstmt.setString(3, deviceName);
			pstmt.setString(4, logMessage);
			pstmt.setTimestamp(5,  java.sql.Timestamp.valueOf(strDate));
			
			r = pstmt.executeUpdate();
			
		} catch (SQLException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			if(conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if(pstmt != null)
				try {
					pstmt.close();
				} catch (SQLException e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}

	public synchronized void run() {

		if(in!=null) {
			
				Connection conn = null;
				CallableStatement cstmt = null;
				ResultSet rs =null;
				
				CallableStatement cstmt2 = null;
				
				int ret =0;
				//Socket  cliSocket = null;
				
				int msgLength = 0;
				String strBytes = "";
				String sendMsg = "";
								
				try {
						byte[] buffer = new byte[8096];
						Arrays.fill(buffer,(byte)0);	
						ret = in.read(buffer, 0, buffer.length);
						if(ret > 0) {
										
							String policyType = "";
							String nodeOrVM = "";
							String policyCode = "";
							String groupYn = "";
							String policySeq = "";
							String uuid = "";
							String ipAddress = "";
							String strData ="";
							String hostName="";
						
							strData = new String((Arrays.copyOfRange(buffer, 0, ret)),"UTF-8");
						
							JSONParser jParser = new JSONParser();
							JSONObject jObject;
																				
							System.out.println("Received Data : " + strData);
							insertLog("", "", "","[정책서버 수신]:" +  strData );
						
							jObject = (JSONObject)jParser.parse(strData.substring(7));
							
							if(jObject.containsKey("policy_type")) 
									policyType = (String)jObject.get("policy_type");
							else if(jObject.containsKey("Policy_type"))
									policyType = (String)jObject.get("Policy_type");
						
							/*PORTAL에서 정책 변경을 통보 받고 변경된 정책의  내용을 NODE와 VM에 전달 */
							if(policyType.toUpperCase().equals("PORTAL_POLICY")) {
							
								nodeOrVM = (String)jObject.get("node_or_vm");
								policyCode = (String)jObject.get("policy_code");
								groupYn = (String)jObject.get("group_Yn");
								policySeq = (String)jObject.get("policy_Seq");
								uuid = (String)jObject.get("uuid");

								Class.forName("org.mariadb.jdbc.Driver");
								conn = DriverManager.getConnection(DBConst.DBConnectionStr,DBConst.userName, DBConst.userPwd);
								
								cstmt = conn.prepareCall("{call SP_GET_" + policyCode.trim() +  "_POLICY_Q(?,?,?,?,?)}"); 
							
								cstmt.setString(1, nodeOrVM);					
								
								cstmt.setString(2, groupYn);
								cstmt.setString(3, policySeq);
								cstmt.setString(4, uuid);
								cstmt.setString(5, ipAddress);
												
								rs = cstmt.executeQuery();					
							
								while(rs.next()){
									
									String  strMsgCheck = "";
									if(policyCode.toUpperCase().equals("PATTERN")) {										
										if(rs.getString("MESSAGE").substring(rs.getString("MESSAGE").length()-2,rs.getString("MESSAGE").length()).equals("&&"))
											strMsgCheck = StringUtils.removeEnd(rs.getString("MESSAGE"),"&&");
										else
											strMsgCheck =rs.getString("MESSAGE");
									}else {
										strMsgCheck = rs.getString("MESSAGE");
									}
																		
									msgLength = strMsgCheck.getBytes("utf-8").length;
									strBytes=String.format("%07d", msgLength);
									//sendMsg=strBytes + rs.getString("MESSAGE");
									sendMsg=strBytes + strMsgCheck;
									
									System.out.println("MESSAGE:" + sendMsg);
									insertLog(nodeOrVM,uuid,"", "[정책조회 결과(Portal 리퀘스트]:"+sendMsg);
									
									Socket cliSocket =  null;
									if(DBConst.mode.equals("TEST")) {
										cliSocket  = new Socket(DBConst.testVM, DBConst.PolicyReceiverPort);
									}else {
										//insertLog(nodeOrVM,uuid,"", "[정책조회 결과(Portal 리퀘스트]: 소켓생성전");
										cliSocket  = new Socket(rs.getString("IP_ADDRESS"), DBConst.PolicyReceiverPort);
										//insertLog(nodeOrVM,uuid,"", "[정책조회 결과(Portal 리퀘스트]: 소켓생성후");
									}
									ClientSender cli = new ClientSender(cliSocket, sendMsg);				
									//insertLog(nodeOrVM,uuid,"", "[정책조회 결과(Portal 리퀘스트]: 보내기전");
									cli.start();
									insertLog(nodeOrVM,uuid,"", "[정책전송완료(Portal->Policy->VM]):"+sendMsg);	
								}
								/* Agent로부터 REQUEST 받은 경우로  NODE와  VM에 대해 해당하는   Policy를 리턴한다. */
							} else {
							  
								hostName = (String)jObject.get("host_name");
								uuid = (String)jObject.get("uuid");
								ipAddress = (String)jObject.get("ip_address");
								
								if(jObject.containsKey("policy_type")) 
									policyType = (String)jObject.get("policy_type");
								else if(jObject.containsKey("Policy_type"))
									policyType = (String)jObject.get("Policy_type");
																
								
								String[] policyArray = policyType.split("_");
								
									Class.forName("org.mariadb.jdbc.Driver");	
									conn = DriverManager.getConnection(DBConst.DBConnectionStr,DBConst.userName, DBConst.userPwd);
									
									/*VM의 전원을 켜자*/
									PreparedStatement stmt = null;
									String sql = "";

									sql = " UPDATE TB_VM " +
										  " SET POWER_STATE='RUNNING' "+							  
										  " WHERE VM_UUID=? ";
										
									stmt = conn.prepareStatement(sql);
									stmt.setString(1, uuid);										
									stmt.executeUpdate();
									stmt.close();
									/*VM의 전원을 켜자*/
									
									cstmt = conn.prepareCall("{call SP_GET_EACH_POLICY_Q(?,?,?,?)}"); 								
									cstmt.setString(1, hostName);					
									cstmt.setString(2, uuid);
									cstmt.setString(3, ipAddress);
									cstmt.setString(4, policyArray[0]);
									
									rs = cstmt.executeQuery();	
										
									if(rs.next()) {
										
										System.out.println("프로시저태운결과:" + rs.getString("MESSAGE"));
										insertLog("",uuid,hostName, "[정책조회 결과(VM 리퀘스트]:"+sendMsg);
										msgLength = rs.getString("MESSAGE").getBytes("utf-8").length;
										strBytes = String.format("%07d", msgLength);
										sendMsg = strBytes + rs.getString("MESSAGE");
										System.out.println("MESSAGE:" + sendMsg);
										SendMessage(socket,sendMsg);
										insertLog(nodeOrVM,uuid,"", "[정책전송완료(VM->Policy->VM)]:"+sendMsg);
										
									}else {
											Class.forName("org.mariadb.jdbc.Driver");	
											//conn = DriverManager.getConnection(DBConst.DBConnectionStr,DBConst.userName, DBConst.userPwd);
											cstmt2 = conn.prepareCall("{call SP_GET_DEFAULT_POLICY(?)}"); 										
											cstmt2.setString(1,policyArray[0]);																									
											rs = cstmt2.executeQuery();
											if(rs.next()) {
												
												System.out.println("디폴트 프로시저태운결과:" + rs.getString("MESSAGE"));
												insertLog("",uuid,hostName, "[정책조회 결과 Default(VM 리퀘스트)]:"+sendMsg);
												msgLength = rs.getString("MESSAGE").getBytes("utf-8").length;
												strBytes = String.format("%07d", msgLength);
												sendMsg = strBytes + rs.getString("MESSAGE");
												System.out.println("MESSAGE:" + sendMsg);
												SendMessage(socket,sendMsg);
												insertLog(nodeOrVM,uuid,"", "[정책전송완료 Default(VM->Policy->VM)]:"+sendMsg);
												cstmt2.close();
											}
									}
								
							} // end of else
						}// end of if
				}// end of try
				catch(SocketException e) {
					try {
						socket.close();
						//break;
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					//break;
				}catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					//break;
				/*}catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					break;
				}catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					break;*/
				}catch(Exception e) {
					e.printStackTrace();
					//break;
				}finally {
					if(conn != null)
						try {
							conn.close();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					if(cstmt != null)
						try {
							cstmt.close();
						} catch (SQLException e) {
						// TODO Auto-generated catch block
							e.printStackTrace();
						}
				}
		}
	} // run
	
	public void SendMessage(java.net.Socket socket, String msg) throws IOException {
		
		DataOutputStream out = null;
		
		try {			
				out = new DataOutputStream(socket.getOutputStream());
			
				byte[] buffer = new byte[msg.getBytes().length];
				buffer = msg.getBytes("utf-8");
			
				System.out.println(msg);
				out.write(buffer);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
} 



/*class ServerSender extends Thread {
Socket socket;
DataOutputStream out;
String name;

ServerSender(String key, Map<String, Client> clients) {
	//this.socket = socket;
    Client c = clients.get(key);
    this.socket = c.getSocket();
    
    try {
		
		out = new DataOutputStream(socket.getOutputStream());
		name = "["+socket.getInetAddress()+":"+socket.getPort()+"]";
	} catch(Exception e) {}
}

public void run() {
	Scanner scanner = new Scanner(System.in);
	while(out!=null) {
		try {
			
			out.writeUTF(name+scanner.nextLine());		
		} catch(IOException e) {}
	}
} // run()
}
*/

/*class Sender extends Thread {
Socket socket;
DataOutputStream out;
String name;

Sender(Socket socket) {
	this.socket = socket;
	try {
		out = new DataOutputStream(socket.getOutputStream());
		name = "["+socket.getInetAddress()+":"+socket.getPort()+"]";
	} catch(Exception e) {}
}

public void run() {
	Scanner scanner = new Scanner(System.in);
	while(out!=null) {
		try {
			out.writeUTF(name+scanner.nextLine());		
		} catch(IOException e) {}
	}
} // run()
}*/
