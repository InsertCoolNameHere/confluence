/*
Copyright (c) 2014, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
*/

package galileo.test.wait;

import java.io.IOException;

import galileo.net.ClientMessageRouter;
import galileo.net.GalileoMessage;
import galileo.net.NetworkDestination;
import galileo.net.Transmission;

public class TransmissionWaitClient {

    private static final int MAX_MSG = 10000;

    private NetworkDestination server;
    private ClientMessageRouter messageRouter;

    public TransmissionWaitClient(NetworkDestination server)
    throws IOException {
        this.server = server;
        messageRouter = new ClientMessageRouter();
    }

    public void send(int numMessages)
    throws InterruptedException, IOException {
        for (int i = 0; i < numMessages; ++i) {
            byte[] payload = new byte[MAX_MSG];
            GalileoMessage msg = new GalileoMessage(payload);
            Transmission t = messageRouter.sendMessage(server, msg);
            t.finish();
        }
    }

    public void disconnect() {
        messageRouter.forceShutdown();
    }

    public static void main(String[] args)
    throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: galileo.test.net.TransmissionWaitClient "
                    + "<server> <num-messages>");
            System.exit(1);
        }

        NetworkDestination server = new NetworkDestination(
                args[0], TransmissionWaitServer.PORT);

        TransmissionWaitClient twc = new TransmissionWaitClient(server);
        twc.send(Integer.parseInt(args[1]));
        twc.disconnect();
    }
}
