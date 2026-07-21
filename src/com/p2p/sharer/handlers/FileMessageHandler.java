package com.p2p.sharer.handlers;

import com.p2p.core.P2PMessage;
import com.p2p.core.PeerConnection;
import com.p2p.core.PeerHandler;
import com.p2p.core.PeerNode;
import com.p2p.sharer.SharerMessage;
import com.p2p.sharer.SharerNode;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FileMessageHandler extends PeerHandler {

    public FileMessageHandler(PeerNode node) {
        super(node);
    }

    @Override
    public void handleMessage(PeerConnection conn, P2PMessage message) {
        SharerNode node = (SharerNode) this.getInstanceNode();
        byte[] payload = message.getMsgDataBytes();
        try {
            ByteArrayInputStream input = new ByteArrayInputStream(payload);
            byte[] lengthBytes = new byte[4];
            input.read(lengthBytes);
            int nameLength = P2PMessage.byteArrayToInt(lengthBytes);
            byte[] nameBytes = new byte[nameLength];
            input.read(nameBytes);
            String fileName = new String(nameBytes, StandardCharsets.UTF_8);
            byte[] fileBytes = input.readAllBytes();
            File target = new File(node.getSharedDirectory(), fileName);
            try (FileOutputStream out = new FileOutputStream(target)) {
                out.write(fileBytes);
            }
            node.recordFile(node.getId(), fileName);
            conn.sendData(new P2PMessage(SharerMessage.REPLY, "FILE RECEIVED"));
        } catch (IOException ex) {
            conn.sendData(new P2PMessage(SharerMessage.ERROR, "FILE ERROR"));
        }
    }
}
