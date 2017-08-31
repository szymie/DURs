import org.szymie.client.strong.optimistic.ClientChannelInitializer;
import org.szymie.client.strong.optimistic.NettyRemoteGateway;
import org.szymie.client.strong.pessimistic.PessimisticClientMessageHandlerFactory;
import org.szymie.messages.Messages;

public class ShowResources {

    public static void main(String[] args) {


        NettyRemoteGateway nettyRemoteGateway = new NettyRemoteGateway(0, new ClientChannelInitializer(new PessimisticClientMessageHandlerFactory()));

        nettyRemoteGateway.connect("127.0.0.1:8080");

        //Messages.ShowResourceRequest request = Messages.ShowResourceRequest.newBuilder().build();
        //Messages.Message message = Messages.Message.newBuilder().setShowResourceRequest(request).build();

        //nettyRemoteGateway.send(message);
    }

}