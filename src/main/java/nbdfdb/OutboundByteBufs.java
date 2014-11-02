package nbdfdb;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by sam on 10/29/14.
 */
public class OutboundByteBufs extends MessageToByteEncoder<ByteBuf> {
  @Override
  protected void encode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, ByteBuf byteBuf2) throws Exception {

  }
}
