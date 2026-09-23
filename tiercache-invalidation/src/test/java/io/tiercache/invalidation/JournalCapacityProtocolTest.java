package io.tiercache.invalidation;

import io.tiercache.*;
import io.tiercache.spi.*;
import io.tiercache.testkit.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class JournalCapacityProtocolTest {
    private int exercise(int capacity, boolean delayed, boolean failReads) throws Exception {
        var data=new InMemoryJournal(capacity);
        InvalidationJournal journal=new InvalidationJournal(){
            public String append(String c,InvalidationMessage m){return data.append(c,m);}
            public List<JournalRow> readRange(String c,String p){return data.readRange(c,p);}
            public String endCursor(String c){return data.endCursor(c);}
            public boolean isTrimmed(String c,String p){return data.isTrimmed(c,p);}
            public CheckedRange checkedRead(String c,String p,int n){
                if(failReads)throw new IllegalStateException("read unavailable");return data.checkedRead(c,p,n);
            }
        };
        var target=new RecoveryProtocolTest.Target();
        var transport=new InMemoryInvalidationTransport(new InMemoryInvalidationTransport.Hub());
        try(var scheduler=new RecoveryProtocolTest.Manual();var service=new InvalidationService(transport,journal,UUID.randomUUID(),InvalidationListener.NOOP,CacheMetricsListener.NOOP,scheduler.now::get)) {
            service.configureRecoveryExecutor(scheduler);service.registerTarget("c",target);
            for(int i=1;i<=128;i++) {
                var message=RecoveryProtocolTest.update("c","k"+i,i);journal.append("c",message);transport.publish(message);
                if(!delayed)scheduler.drain();
                if(i==64 && !delayed && !failReads)assertEquals("64",RecoveryProtocolTest.field(RecoveryProtocolTest.state(service,"c"),"cursor"));
            }
            scheduler.drain();
            if (failReads) {
                // A failed live tick retains the cursor. Recovery then uses its
                // existing conservative reset path, with the normal retry delay.
                assertEquals("0",RecoveryProtocolTest.field(RecoveryProtocolTest.state(service,"c"),"cursor"));
                service.recoverAsync(Runnable::run);
                scheduler.advance(1); scheduler.drain();
            }
            assertEquals("128",RecoveryProtocolTest.field(RecoveryProtocolTest.state(service,"c"),"cursor"));
            return target.clears.get();
        }
    }
    @Test void exactRetentionNeedsCursorPlusSixtyFourLaterEvents() throws Exception {
        assertEquals(64,JournalProtocol.CURSOR_CADENCE);assertEquals(65,JournalProtocol.MIN_CAPACITY);
        assertEquals(1,exercise(64,false,false));assertEquals(0,exercise(65,false,false));
    }
    @Test void validCapacityDoesNotGuaranteeRetentionUnderDelayedRecovery() throws Exception {
        assertTrue(exercise(65,true,false)>0);
    }
    @Test void validCapacityDoesNotBypassConservativeHandlingOfReadFailure() throws Exception {
        assertTrue(exercise(65,false,true)>0);
    }
}
