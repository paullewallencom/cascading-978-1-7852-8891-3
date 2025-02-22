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

import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/*
 * This SubAssembly combines named entity extraction for people, organizations and chemicals. Please note
 * that a groupBy pipe cannot be split, so we are creating individual groupBy pipes for every entity type
 */
public class NamedEntityExtractorHDFS extends SubAssembly
{
	public NamedEntityExtractorHDFS (Pipe inPipe)
  	 {
		Pipe groupPipe1 = new GroupBy(inPipe, new Fields("documentname","sentnumber"), new Fields("wordnum"));
		Pipe groupPipe2 = new GroupBy(inPipe, new Fields("documentname","sentnumber"), new Fields("wordnum"));
		Pipe groupPipe3 = new GroupBy(inPipe, new Fields("documentname","sentnumber"), new Fields("wordnum"));
		/* Please supply your correct hdfs directory here. The hardcoded paths will not work in your environment unless unless
		 * you put your models and dictionaries in these exact locations
		 */
		String pathPrefix = "hdfs:/user/" + System.getProperty("user.name") + "/";
		Pipe peoplePipe = new Every(groupPipe1, new NameBuffer<NameBuffer.Context>(pathPrefix + "models/en-ner-person.bin", false,"person"), Fields.RESULTS);	
		Pipe orgPipe = new Every(groupPipe2, new NameBuffer<NameBuffer.Context>(pathPrefix + "models/en-ner-organization.bin", false,"organization"), Fields.RESULTS);	
		Pipe dictionaryPipe = new Every(groupPipe3, new NameBuffer<NameBuffer.Context>(pathPrefix + "dictionaries/chemicals.csv", true,"chemical"), Fields.RESULTS);	
		setPrevious(inPipe);
		setTails(peoplePipe, orgPipe, dictionaryPipe);
  	 }


}
