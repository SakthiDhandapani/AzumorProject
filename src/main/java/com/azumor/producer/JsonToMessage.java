package com.azumor.producer;

import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import com.azumor.protobuff.PersonProtos;
import com.azumor.protobuff.PersonProtos.Person;

/**
 * This class converts Json String to ProtoBuf
 *
 */
public class JsonToMessage {	
	public static Message parseJson(String jsonString) {
		Person.Builder builder = PersonProtos.Person.newBuilder();
		try {
			JsonFormat.merge(jsonString, builder);
		} catch (com.googlecode.protobuf.format.JsonFormat.ParseException e) {
			System.out.println("Error occured :: "+e.getMessage());
			System.out.println("Invalid format exception occured");			
		}
		return builder.build();
	}
}
