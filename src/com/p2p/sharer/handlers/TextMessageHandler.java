package com.p2p.sharer.handlers;

import com.p2p.core.P2PMessage;
import com.p2p.core.PeerConnection;
import com.p2p.core.PeerHandler;
import com.p2p.core.PeerNode;
import com.p2p.sharer.SharerMessage;
import com.p2p.sharer.SharerNode;

public class TextMessageHandler extends PeerHandler {

    public TextMessageHandler(PeerNode node) {
        super(node);
    }

    @Override
    public void handleMessage(PeerConnection conn, P2PMessage message) {
        SharerNode node = (SharerNode) this.getInstanceNode();
        String payload = message.getMsgData();
        node.recordMessage("peer", payload);
        conn.sendData(new P2PMessage(SharerMessage.REPLY, "TEXT RECEIVED"));
    }
}
