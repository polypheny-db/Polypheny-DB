package org.polypheny.db.mql;

import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlCount extends MqlCollectionStatement {

    private final boolean isEstimate;
    private final BsonDocument document;


    public MqlCount( String collection, BsonDocument document ) {
        this( collection, document, false );
    }


    public MqlCount( String collection, BsonDocument document, boolean isEstimate ) {
        super( collection );
        this.document = document;
        this.isEstimate = isEstimate;
    }


    @Override
    public Type getKind() {
        return Type.COUNT;
    }

}