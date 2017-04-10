/*
Copyright (c) 2014, Colorado State University
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

This software is provided by the copyright holders and contributors "as is" and
any express or implied warranties, including, but not limited to, the implied
warranties of merchantability and fitness for a particular purpose are
disclaimed. In no event shall the copyright holder or contributors be liable for
any direct, indirect, incidental, special, exemplary, or consequential damages
(including, but not limited to, procurement of substitute goods or services;
loss of use, data, or profits; or business interruption) however caused and on
any theory of liability, whether in contract, strict liability, or tort
(including negligence or otherwise) arising in any way out of the use of this
software, even if advised of the possibility of such damage.
*/

package galileo.dataset;

import java.io.IOException;

import galileo.serialization.ByteSerializable;
import galileo.serialization.SerializationException;
import galileo.serialization.SerializationInputStream;
import galileo.serialization.SerializationOutputStream;

/**
 * The basic unit of storage in Galileo. A block could be a file, collection of
 * metadata, etc.
 *
 * @author malensek
 */
public class Block implements ByteSerializable {

	private String filesystem;
	private Metadata metadata;
	private byte[] data;

	public Block(String filesystem, byte[] data) {
		this.filesystem = filesystem;
		this.metadata = new Metadata();
		this.data = data;
	}

	public Block(String filesystem, Metadata metadata, byte[] data) {
		this.filesystem = filesystem;
		this.metadata = metadata;
		this.data = data;
	}

	public String getFilesystem() {
		return this.filesystem;
	}

	public Metadata getMetadata() {
		return metadata;
	}

	public byte[] getData() {
		return data;
	}

	@Deserialize
	public Block(SerializationInputStream in) throws IOException, SerializationException {
		this.filesystem = in.readString();
		if(in.readBoolean())
        	this.metadata = new Metadata(in);
        if(in.readBoolean())
        	this.data = in.readField();
	}

	@Override
	public void serialize(SerializationOutputStream out) throws IOException {
		out.writeString(filesystem);
		out.writeBoolean(this.metadata != null);
		if (this.metadata != null)
			out.writeSerializable(metadata);
		out.writeBoolean(this.data != null);
		if(this.data != null)
			out.writeField(this.data);
	}
}
