/* Copyright (c) 2015, Analytics Inside LLC. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *   - Neither the name of Analytics Inside LLC or the names of its
 *     products or trademarks,  may be used to endorse or promote products 
 *     derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */ 

package com.ai.learning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import opennlp.tools.namefind.TokenNameFinderModel;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
/*
 * This Cascading buffer is used to extract named entities using either OpenNLP models or user-supplied
 * dictionaries
 */
public class NameBuffer<Context> extends BaseOperation<NameBuffer.Context> implements Buffer<NameBuffer.Context>
{
	static Fields fieldInput = new Fields( "documentname","sentnumber", "wordnum", "word");
	static Fields fieldOutput = new Fields( "documentname","sentnumber", "word","type");
	String modelName="";
	Boolean isDict=false;
	String type="";

	public class Context
	{
		TokenNameFinderModel model=null;
		HashSet<String> dictionary=null;
		String modelName ="";
		Boolean isDict=false;
		String type="";
	}
	
	// Decide if you are using OpenNLP model or a dictionary
	// if using a dictionary then isDict is true, and modelName becomes dictionary name
	public NameBuffer(String model, Boolean dict) {
		super(4, fieldOutput);
		modelName=model;
		isDict=dict;
		type="unknown";
	}

	public NameBuffer(String model, Boolean dict, String entityType) {
		super(4, fieldOutput);
		modelName=model;
		isDict=dict;
		type= entityType;
	}

	@Override
	public void prepare (FlowProcess flowProcess, OperationCall<NameBuffer.Context> bufferCall)
	{
		bufferCall.setContext(new NameBuffer.Context());
		Context context = bufferCall.getContext();
		context.modelName=modelName;
		context.type=type;
		context.isDict=isDict;
		if (context.isDict==false)
		{
			try {
				context.model = Utilities.getNamedEntityModel(context.modelName);
				// Use this for debugging
				System.out.println("Model loaded");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else 
		{
			try {
				context.dictionary = Utilities.getDictionary(context.modelName);
				System.out.println("Dictionary loaded");
			} catch (Exception  e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
	}

	public void cleanup(FlowProcess flowProcess, BufferCall<NameBuffer.Context> bufferCall)
	{
		bufferCall.setContext(null); 
	}

	public void operate( FlowProcess flowProcess, BufferCall<NameBuffer.Context> bufferCall )
	{
		TupleEntry group = bufferCall.getGroup();
		List<NamedEntity> NamedEntityArr= new ArrayList<NamedEntity>() ;

		// get all the current argument values for this grouping
		Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();

		// create a Tuple to hold our result values and set its document name field
		Tuple result = Tuple.size(4);
		Context context = bufferCall.getContext();
		List<String> tokenList= new ArrayList<String>() ; 

		while( arguments.hasNext() )
		{
			Tuple tuple = arguments.next().getTuple();
			tokenList.add( tuple.getString(3));
		}	

		String[] tokens = new String[tokenList.size()];
		tokens = tokenList.toArray(tokens);
		if (context.isDict==false)
		{
			try {
				NamedEntityArr = Utilities.detectNamedEntity(context.model, tokens,group.getString("documentname"),context.type );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else
		{
			try {
				NamedEntityArr = Utilities.detectDictionary(context.dictionary, tokens,group.getString("documentname"),context.type );
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}	

		for (int i = 0; i < NamedEntityArr.size(); i++)
		{
			result.set(0, group.getString("documentname"));
			result.set(1, group.getString("sentnumber"));
			result.set(2, NamedEntityArr.get(i).getName());
			result.set(3, NamedEntityArr.get(i).getType());

			// Return the result Tuple
			bufferCall.getOutputCollector().add( result );
		}
	}
}
