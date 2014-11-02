package nbdfdb;

import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

/**
 * Export an NBD volume
 */
public class NegotiationHandler extends ChannelInboundHandlerAdapter {

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf byteBuf = (ByteBuf) msg;
    try {
      if (byteBuf.readableBytes() >= 4 + 8 + 4 + 4) {
        System.out.println("0: " + (byteBuf.readInt() == 0));
        System.out.println("0x49484156454F5054: " + (byteBuf.readLong() == 0x49484156454F5054l));
        System.out.println("NBD_OPT_EXPORT_NAME: " + (byteBuf.readInt() == 1));
        int length = byteBuf.readInt();
        System.out.println("Length: " + length);
        byte[] bytes = new byte[length];
        if (byteBuf.readableBytes() >= length) {
          byteBuf.readBytes(bytes);
          System.out.println("Name: " + new String(bytes));
          ctx.write(Unpooled.copiedBuffer(Longs.toByteArray(Long.MAX_VALUE)));
          ctx.writeAndFlush(Unpooled.copiedBuffer(Shorts.toByteArray((short) 3)));
          ctx.pipeline().remove(this);
          ctx.pipeline().addLast(new ClientFlags());
          ctx.pipeline().addLast(new ExportHandler(new String(bytes)));
        }
      }
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }
}
