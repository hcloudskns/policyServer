
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
				serverSocket = new ServerSocket(8888);
				System.out.println("Polish 서버가 준비되었습니다.");
				
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

	public synchronized void run() {
		while(in!=null) {
			
				Connection conn = null;
				CallableStatement cstmt = null;
				ResultSet rs =null;
				int ret =0;
				Socket  cliSocket = null;
				try {
					
					byte[] buffer = new byte[8096];
					ret = in.read(buffer, 0, buffer.length);
					int rowCount = 0;
					//System.out.println(ret);
										
					String policyType = "";
					String nodeOrVM = "";
					String policyCode = "";
					String defaultYn = "";
					String hostName = "";
					String uuid = "";
					String ipAddress = "";
					String strData ="";
					
					strData = new String((Arrays.copyOfRange(buffer, 0, ret)),"UTF-8");
										
					JSONParser jParser = new JSONParser();
					JSONObject jObject;
																			
					System.out.print("Received Data From Client: " + strData);
					
					jObject = (JSONObject)jParser.parse(strData);
					policyType = (String)jObject.get("policy_type");
					
					/*PORTAL에서 정책 변경을 통보 받고 변경된 정책의  내용을 NODE와 VM에 전달 */
					if(policyType.toUpperCase().equals("PORTAL_POLICY")) {
						
						nodeOrVM = (String)jObject.get("node_or_vm");
						policyCode = (String)jObject.get("policy_code");
						defaultYn = (String)jObject.get("default_yn");
						hostName = (String)jObject.get("host_name");
						uuid = (String)jObject.get("uuid");
						ipAddress = (String)jObject.get("ip_address");
						
						Class.forName("org.mariadb.jdbc.Driver");	
						conn = DriverManager.getConnection("jdbc:mariadb://192.168.101.110:3306/HCLOUD","root", "P@$$w0rd");
					
						cstmt = conn.prepareCall("{call SP_GET_" + policyCode.trim() +  "_POLICY_Q(?,?,?,?,?)}"); 
					
						cstmt.setString(1, nodeOrVM);					
						cstmt.setString(2, defaultYn);
						cstmt.setString(3, hostName);
						cstmt.setString(4, uuid);
						cstmt.setString(5, ipAddress);
										
						rs = cstmt.executeQuery();					
						
						while(rs.next()){
							 
							//cliSocket = new Socket(rs.getString("IP_ADDRESS"), 10001);
							cliSocket = new Socket("localhost", 10001);
							ClientSender cli = new ClientSender(cliSocket, rs.getString("MESSAGE"));
							cli.start();
															
						}
						
					/* Agent로부터 REQUEST 받은 경우로  NODE와  VM에 대해 해당하는   Policy를 리턴한다. */
					}else {
						  
						hostName = (String)jObject.get("host_name");
						uuid = (String)jObject.get("uuid");
						ipAddress = (String)jObject.get("ip_address");
						policyType = (String)jObject.get("policy_type");
						String[] policyArray = policyType.split("_");
						
						Class.forName("org.mariadb.jdbc.Driver");	
						conn = DriverManager.getConnection("jdbc:mariadb://192.168.101.110:3306/HCLOUD","root", "P@$$w0rd");
					
						cstmt = conn.prepareCall("{call SP_GET_EACH_POLICY_Q(?,?,?,?)}"); 
					
						cstmt.setString(1, hostName);					
						cstmt.setString(2, uuid);
						cstmt.setString(3, ipAddress);
						cstmt.setString(4, policyArray[0]);
						
						rs = cstmt.executeQuery();	
						

						while(rs.next()){							 
							System.out.print("프로시저태운결과:" + rs.getString("MESSAGE"));
							SendMessage(socket,rs.getString("MESSAGE"));									
						}
											
					}
																				
				}
				catch(SocketException e) {
					
					try {
						socket.close();
						break;
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
				catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					break;
				}catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					break;
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
					break;
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
			
				System.out.print(msg);
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
