package com.p2p.sharer.handlers;

import com.p2p.core.P2PMessage;
import com.p2p.core.PeerConnection;
import com.p2p.core.PeerHandler;
import com.p2p.core.PeerNode;
import com.p2p.sharer.SharerMessage;

/**
 * Handler for NAME messages.
 * NAME: Requests a peer to reply with its official peer id.
 */
public class NameHandler extends PeerHandler {

    public NameHandler(PeerNode node) {
        super(node);
    }

    @Override
    public void handleMessage(PeerConnection conn, P2PMessage message) {
        PeerNode peer = this.getInstanceNode();
        conn.sendData(new P2PMessage(SharerMessage.REPLY, peer.getId()));
    }
}
