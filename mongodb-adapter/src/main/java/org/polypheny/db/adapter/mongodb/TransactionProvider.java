/*
 * Copyright 2019-2021 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.adapter.mongodb;

import com.mongodb.MongoClientException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import lombok.Setter;

public class TransactionProvider {

    private final MongoClient client;
    @Setter
    private ClientSession session;


    public TransactionProvider( MongoClient client ) {
        this.client = client;
        this.session = client.startSession();
    }


    public void startTransaction() {
        if ( session.hasActiveTransaction() ) {
            throw new RuntimeException( "There is an uncommitted transaction in the store." );
        }

        session = client.startSession();
        session.startTransaction();
    }


    public void commit() {
        if ( session.hasActiveTransaction() ) {
            try {
                session.commitTransaction();
            } catch ( MongoClientException e ) {
                session.abortTransaction();
            } finally {
                session.close();
            }
        }
    }


    public void rollback() {
        if ( session.hasActiveTransaction() ) {
            session.abortTransaction();
        }

        session.close();
    }


    public ClientSession getSession() {
        return session;
    }

}
