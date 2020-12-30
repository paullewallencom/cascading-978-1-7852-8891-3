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

import java.util.Iterator;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/*
 * Given a tuple stream of sentences this Cascading Buffer utilizes Utilities.getTokens function
 * to break a sentence into tokens. Note that is also fixes the punctuation, i.e. makes sure that
 * abbreviations such as Mr., Ms., and so on, are not counted as the end of the sentence.
 */
public class TokenBuffer<Context> extends BaseOperation<Context> implements Buffer<Context>
{
	static Fields fieldDeclaration = new Fields( "document","sentnum", "sentence");
	static Fields fieldOutput = new Fields( "documentname","sentnumber", "wordnum", "word");

	public TokenBuffer() {
		super(3, fieldOutput);
	}
	public void operate( FlowProcess flowProcess, BufferCall bufferCall )
	{
		int sentnum = 0;
		TupleEntry group = bufferCall.getGroup();

		// get all the current argument values for this grouping
		Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();

		// create a Tuple to hold our result values and set its document name field
		Tuple result = Tuple.size(4);

		int token_count = 0;
		while( arguments.hasNext() )
		{
			Tuple tuple = arguments.next().getTuple();
			String[] tokens = Utilities.getTokens(tuple.getString(2));
			for (int i = 0; i < tokens.length; i++)
			{
				String token = tokens[i];
				if (token == null || token.isEmpty())
					continue;
				token_count++;
				result.set(0, group.getString("document"));
				result.set(1, sentnum);
				result.set(2, token_count);
				result.set(3, token);

				// Return the result Tuple
				bufferCall.getOutputCollector().add( result );
				// See if the last token is an abbreviation. If so, treat the next sentence as a 
				// continuation. If not, increment our sentence number and reset the word number.
				if (i == tokens.length - 1)
				{
					if (!Utilities.isAbbreviation(token))
					{
						sentnum++;
						token_count = 0;
					}
				}
			}
		}
	}
}
