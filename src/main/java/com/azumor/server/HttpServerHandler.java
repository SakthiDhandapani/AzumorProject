package com.azumor.server;

import com.azumor.producer.JsonToMessage;
import com.azumor.producer.KafkaMsgProducer;
import com.google.protobuf.Message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

public class HttpServerHandler extends SimpleChannelInboundHandler<Object> {

	private HttpRequest request;


	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
		if (msg instanceof HttpRequest) {
			this.request = (HttpRequest) msg;

		}

		if (msg instanceof HttpContent) {
			HttpContent httpContent = (HttpContent) msg;

			String queryParam = httpContent.content().toString(CharsetUtil.UTF_8);
			if (msg instanceof LastHttpContent) {
							
				try {
					
					String uri[] = request.uri().split("/");
					if (uri.length == 2) {
						Message message = JsonToMessage.parseJson(queryParam);
						String topic = uri[1];
						KafkaMsgProducer.publish(message, topic);
						ByteBuf content = Unpooled.copiedBuffer("Message sent in kafka Queued", CharsetUtil.UTF_8);
						FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
								HttpResponseStatus.OK, content);
						response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
						response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
						ctx.write(response);
					} else {
						FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
								HttpResponseStatus.REQUEST_URI_TOO_LONG, Unpooled.copiedBuffer(
										"Invalid uri" + "uri should be /{messageName}/", CharsetUtil.UTF_8));

						ctx.write(response);
					}
				} catch (Exception e) {
					System.out.println("Error " + e.getMessage());
					FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
							HttpResponseStatus.BAD_REQUEST, Unpooled.copiedBuffer(e.getMessage(), CharsetUtil.UTF_8));
					ctx.write(response);

				}
			}
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}
}
