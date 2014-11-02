package nbdfdb;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public class ClientFlags extends ChannelInboundHandlerAdapter {
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf bb = (ByteBuf) msg;
    try {
      if (bb.readableBytes() == 2) {
        System.out.println("Client flags: " + (bb.readShort() == 3));
        ctx.pipeline().remove(this);
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }
}
