/*
 * Copyright (c) 2010 Luminis
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 * - Neither the name of the Luminis nor the names of its contributors may be
 *   used to endorse or promote products derived from this software without
 *   specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#import "Changes.h"
#import "JSON.h"

#define kChangesURL @"http://%@:%d/%@/_changes?filter=%@&feed=continuous&include_docs=true"
#define kChangesNoFilterURL @"http://%@:%d/%@/_changes?feed=continuous&include_docs=true"
#define kLastSeqURL @"http://%@:%d/%@"



@implementation Changes

@synthesize delegate;
@synthesize m_port;

-(id)initWithHost: (NSString*) host database: (NSString*) database andFilter:(NSString*)filter {
    self = [super init];
    if (self) {
        m_host = host;
        m_database = database;
        m_filter = filter;
        m_port = 5984;
    }
    return self;
}

-(id)initWithHost: (NSString*) host andDatabase: (NSString*) database {
    self = [self initWithHost: host database: database andFilter: nil];
    return self;
}


/*
 * get the latest update_seq from the database.
 */
-(int)last_seq {
    
    NSError *error;
    NSURL *url = [NSURL URLWithString:[NSString stringWithFormat: kLastSeqURL, m_host, self.port, m_database]];
    NSString *data = [NSString stringWithContentsOfURL: url encoding: NSUTF8StringEncoding error: &error];
    if (data) {
//        NSLog(@"data: %@", data);
        id json = [data JSONValue];
        if (json) {
            NSDictionary *dict = (NSDictionary*)json;
            NSObject *seq = [dict objectForKey: @"update_seq"];
            if (seq) {
                return [(NSNumber*)seq intValue];
            }
        }
    }
    return 0;
}

/*
 * create the changes feed url
 */
-(NSURL*)buildURL:(int)seq {
    NSString *str = nil;
    
    if (m_filter) {
        str = [NSString stringWithFormat: kChangesURL, m_host, self.port, m_database, m_filter];
    } else {
        str = [NSString stringWithFormat: kChangesNoFilterURL, m_host, self.port, m_database];
    }
    str = [NSString stringWithFormat: @"%@&since=%d", str, seq];
    return [NSURL URLWithString: str];
}

/*
 * start the connection for the next change from the feed
 */
-(void)requestChanges:(int) seq {

    theURL = [[self buildURL: seq] retain];
    
    NSLog(@"Changes URL: %@", theURL);
    
    //open the socket
    CFReadStreamRef readStream;
    CFWriteStreamRef writeStream;
    CFStreamCreatePairWithSocketToHost(NULL, (CFStringRef)[theURL host], [[theURL port] intValue], &readStream, &writeStream);
    
    inputStream = (NSInputStream *)readStream;
    outputStream = (NSOutputStream *)writeStream;
    
    [inputStream setDelegate: self];
    [outputStream setDelegate :self];
    [inputStream scheduleInRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode];
    [outputStream scheduleInRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode];
    [inputStream open];
    [outputStream open];    
    
    
    NSLog(@"connection started");
}


/* stream eventing */
- (void)stream:(NSStream *)stream handleEvent:(NSStreamEvent)eventCode {
    
    switch(eventCode) {
        case NSStreamEventOpenCompleted:
            [self connection: nil didReceiveResponse: nil];
			break;
            
		case NSStreamEventErrorOccurred:
            [self connection: nil didFailWithError: [stream streamError]];
			break;
            
		case NSStreamEventEndEncountered:
            [stream close];
            [stream removeFromRunLoop:[NSRunLoop currentRunLoop] forMode:NSDefaultRunLoopMode];
            [stream release];
            inputStream = nil;
            outputStream = nil;
            [self connectionDidFinishLoading: nil];
			break;            
            
        case NSStreamEventHasSpaceAvailable:
        {
            if (stream == outputStream) {
                [self connection: nil willSendRequest: [NSURLRequest requestWithURL: theURL] redirectResponse: nil];
                
                NSString *str = [NSString stringWithFormat: @"GET %@?%@ HTTP/1.0\r\n\r\n", [theURL path], [theURL query]];
                const uint8_t *rawstring = (const uint8_t *)[str UTF8String];
                [outputStream write:rawstring maxLength: [str length]];
                [outputStream close];
            }
        }
            break;
		case NSStreamEventHasBytesAvailable:
        {
			if (stream == inputStream)
			{
				uint8_t buffer[1024];
				int len;
				while ([inputStream hasBytesAvailable])
				{
					len = [inputStream read: buffer maxLength: sizeof(buffer)];
					if (len > 0)
					{
						NSData *theData = [[NSData alloc] initWithBytes: buffer length:len];
                        [self connection: nil didReceiveData: theData];
					}
				}
			}
        }
            break;
    }
}

/* NSURLConnection delegate implementation */

- (NSURLRequest *)connection:(NSURLConnection *)connection willSendRequest:(NSURLRequest *)request redirectResponse:(NSURLResponse *)response {
    //NSLog(@"willSendRequest: %@", [request URL]);
    return request;
}

- (void)connection:(NSURLConnection *)connection didReceiveResponse:(NSURLResponse *)response {
    //NSLog(@"didReceive Response");
}

- (void)connection:(NSURLConnection *)connection didReceiveData:(NSData *)data {
    //NSLog(@"didReceive data");
    NSString *str = [[NSString alloc] initWithData:data encoding: NSUTF8StringEncoding];
    
    NSArray *ary = [str componentsSeparatedByString:@"\n"];
    for (NSString *line in ary) {
        if ([line length] > 0 && [line characterAtIndex: 0] == '{') {
            //NSLog(@"data: %@", line);
            id json = [line JSONValue];

            NSDictionary *dict = (NSDictionary*)json;
            NSObject *seq = [dict objectForKey: @"seq"];
            
            if (seq && delegate) {
                //NSLog(@"delegate notified");
                [delegate changeReceived: dict];
            }
            NSObject *last_seq = [dict objectForKey: @"last_seq"];
            if (last_seq && delegate) {
                int last = [((NSNumber*)last_seq) intValue];
                //NSLog(@"delegate notified");
                [delegate lastSequence: last];
            }
        }
    }

}

- (void)connectionDidFinishLoading:(NSURLConnection *)connection {
    //NSLog(@"DidFinishLoading");
    [delegate changesComplete: nil];
}

- (void)connection:(NSURLConnection *)connection didFailWithError:(NSError *)error {
    //NSLog(@"didFailWithError");
    [delegate changesComplete: error];
}



@end
