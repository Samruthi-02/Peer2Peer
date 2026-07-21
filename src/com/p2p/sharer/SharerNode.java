package com.p2p.sharer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import com.p2p.core.P2PMessage;
import com.p2p.core.PeerInfo;
import com.p2p.core.PeerNode;
import com.p2p.core.util.LoggerUtil;
import com.p2p.sharer.handlers.FileGetHandler;
import com.p2p.sharer.handlers.FileMessageHandler;
import com.p2p.sharer.handlers.JoinHandler;
import com.p2p.sharer.handlers.ListHandler;
import com.p2p.sharer.handlers.NameHandler;
import com.p2p.sharer.handlers.QueryHandler;
import com.p2p.sharer.handlers.QueryResponseHandler;
import com.p2p.sharer.handlers.QuitHandler;
import com.p2p.sharer.handlers.Router;
import com.p2p.sharer.handlers.TextMessageHandler;
/**
 * Backend Implementation for a Sharer File Node
 * */
public class SharerNode extends PeerNode {
	private Hashtable<String,String> files;
	private final List<String> incomingMessages;
	private final List<String> incomingFiles;
	private final File sharedDirectory;
	
	public SharerNode(int maxPeers, PeerInfo myInfo){
		super(maxPeers,myInfo);
		this.files = new Hashtable<String,String>();
		this.incomingMessages = new ArrayList<String>();
		this.incomingFiles = new ArrayList<String>();
		this.sharedDirectory = new File(System.getProperty("user.dir"), "shared");
		this.sharedDirectory.mkdirs();
		this.addRouter(new Router(this));
		this.addHandlers();
		
	}
	private void addHandlers(){
		this.addHandler(SharerMessage.INSERT_PEER,new JoinHandler(this));
		this.addHandler(SharerMessage.LIST_PEER, new ListHandler(this));
		this.addHandler(SharerMessage.PEER_NAME, new NameHandler(this));
		this.addHandler(SharerMessage.QUERY,new QueryHandler(this));
		this.addHandler(SharerMessage.QUERY_RESPONSE, new QueryResponseHandler(this));
		this.addHandler(SharerMessage.FILE_GET, new FileGetHandler(this));
		this.addHandler(SharerMessage.TEXT_MESSAGE, new TextMessageHandler(this));
		this.addHandler(SharerMessage.FILE_MESSAGE, new FileMessageHandler(this));
		this.addHandler(SharerMessage.PEER_QUIT, new QuitHandler(this));
	}
	
	public void addLocalFile(String filename){
		if(this.files.containsKey(filename)){
			this.files.remove(filename);
		}
		this.files.put(filename, this.getId());
	}
	
	public String[] getFileNames(){
		return this.files.keySet().toArray(new String[this.files.size()]);
	}
	public String getFileOwner(String filename){
		return files.get(filename);
	}
	
	public boolean buildPeers(String host, int port, int hops){
		LoggerUtil.getLogger().fine("build peers");
		if(this.maxPeerReached() || hops <= 0){
			return false;
		}
		
		PeerInfo currentPeer = new PeerInfo(host,port);
		List<P2PMessage> responseList = this.connectAndSend(currentPeer, SharerMessage.PEER_NAME, "", true);
		if(responseList == null || responseList.size() == 0){
			return false;
		}
		
		String peerId = responseList.get(0).getMsgData();
		LoggerUtil.getLogger().fine("Connected with " + peerId);
		currentPeer.setId(peerId);
		String dataMsg = String.format("%s %s %d",this.getId(),this.getHost(),this.getPort());
		List<P2PMessage> insertReplies = this.connectAndSend(currentPeer, SharerMessage.INSERT_PEER,dataMsg,true);
		if(insertReplies == null || insertReplies.size() == 0){
			return false;
		}
		String respMsgType = insertReplies.get(0).getMsgType();
		
		if(!respMsgType.equals(SharerMessage.REPLY) || this.getPeerKeys().contains(peerId)){
			return false;
		}
		
		this.addPeer(currentPeer);
		
		//DFS to add mode peers
		
		responseList = this.connectAndSend(currentPeer, SharerMessage.LIST_PEER, "", true);
		if(responseList != null && responseList.size() > 1){
			responseList.remove(0);
			for(P2PMessage msg: responseList){
				String data []  = msg.getMsgData().split("\\s");
				String nextPId = data[0];
				String nextHost = data[1];
				int nextPort = Integer.parseInt(data[2]);
				if(this.getId().equals(nextPId)){
					buildPeers(nextHost, nextPort, hops-1);
				}
			}
		}
		return true;
	}
	
	public Hashtable<String,String> getTableFiles(){
		return this.files;
	}

	@Override
	public boolean sendTextMessage(String peerId, String message) {
		PeerInfo peer = this.getPeer(peerId);
		if (peer == null || message == null || message.trim().isEmpty()) {
			return false;
		}
		List<P2PMessage> replies = this.connectAndSend(peer, SharerMessage.TEXT_MESSAGE, message, true);
		return replies.stream().anyMatch(reply -> SharerMessage.REPLY.equals(reply.getMsgType()));
	}

	@Override
	public boolean sendFile(String peerId, String filePath) {
		PeerInfo peer = this.getPeer(peerId);
		if (peer == null || filePath == null || filePath.trim().isEmpty()) {
			return false;
		}
		File source = new File(filePath);
		if (!source.exists() || !source.isFile()) {
			return false;
		}
		try {
			byte[] payload = buildFilePayload(source);
			List<P2PMessage> replies = this.connectAndSendBytes(peer, SharerMessage.FILE_MESSAGE, payload, true);
			return replies.stream().anyMatch(reply -> SharerMessage.REPLY.equals(reply.getMsgType()));
		} catch (IOException ex) {
			LoggerUtil.getLogger().warning("Unable to send file: " + ex.getMessage());
			return false;
		}
	}

	@Override
	public List<String> getIncomingMessages() {
		return this.incomingMessages;
	}

	@Override
	public List<String> getIncomingFiles() {
		return this.incomingFiles;
	}

	public void recordMessage(String sender, String message) {
		this.incomingMessages.add("[" + sender + "] " + message);
	}

	public void recordFile(String sender, String fileName) {
		this.incomingFiles.add("[" + sender + "] " + fileName);
	}

	public File getSharedDirectory() {
		return this.sharedDirectory;
	}

	private byte[] buildFilePayload(File file) throws IOException {
		byte[] nameBytes = file.getName().getBytes(StandardCharsets.UTF_8);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(P2PMessage.intToByteArray(nameBytes.length));
		out.write(nameBytes);
		out.write(Files.readAllBytes(file.toPath()));
		return out.toByteArray();
	}
	
}
