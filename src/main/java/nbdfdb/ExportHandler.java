package nbdfdb;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

/**
 * Created by sam on 10/28/14.
 */
public class ExportHandler extends ReplayingDecoder<Command> {
  private final String name;

  public ExportHandler(String name) {
    this.name = name;
  }

  @Override
  protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
    System.out.println(byteBuf.readableBytes());
    System.out.println("Reading stuff for " + name);
  }
}
