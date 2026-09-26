package io.tiercache.tck.vt;

import jdk.jfr.*;
import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

/** Test-only recording validation and conservative attribution, also tested on Java 17. */
public final class VtEvidence {
    public static final String PIN = "jdk.VirtualThreadPinned";
    @Name("tiercache.VtEvidence") @StackTrace(false)
    public static final class Proof extends Event { public String runId; }

    public static void settings(Map<String,String> settings, String event) {
        if (!"true".equals(settings.get(event+"#enabled"))
                || !"true".equals(settings.get(event+"#stackTrace"))
                || !Set.of("0 ns","0 ms","0 s").contains(settings.getOrDefault(event+"#threshold","missing"))) {
            throw new IllegalStateException("Missing enabled, zero-threshold, stack-bearing recording settings: "+settings);
        }
    }
    public static String category(List<String> frames) {
        if (frames == null || frames.isEmpty()) throw new IllegalStateException("Unclassifiable pinned event: missing stack");
        boolean test = false;
        for (String frame : frames) {
            String name=frame.replace('/','.');
            if (name.startsWith("io.tiercache.tck.") || name.startsWith("io.tiercache.testkit.")) test=true;
            else if (name.startsWith("io.tiercache.")) return "product";
        }
        return test ? "testOnly" : "jdkOrOther";
    }
    public static Map<String,Object> read(Path path, String runId, String eventName) throws Exception {
        long total=0, product=0, test=0, other=0, sum=0, max=0, proofs=0;
        var reasons=new TreeMap<String,Long>(); var operations=new TreeMap<String,Long>();
        var examples=new ArrayList<Object>();
        Map<String,String> optionalFields;
        try (var file=new RecordingFile(path)) {
            var type=file.readEventTypes().stream().filter(t -> t.getName().equals(eventName)).findFirst()
                    .orElseThrow(() -> new IllegalStateException("Requested pinning event metadata absent"));
            optionalFields=new TreeMap<>();
            for(String field:List.of("pinnedReason","blockingOperation"))
                optionalFields.put(field,type.getFields().stream().anyMatch(f->f.getName().equals(field))?"available":"unavailable");
            while (file.hasMoreEvents()) {
                RecordedEvent event=file.readEvent();
                if (event.getEventType().getName().equals("tiercache.VtEvidence")) {
                    if (!runId.equals(event.getString("runId"))) throw new IllegalStateException("Recording run identity mismatch");
                    proofs++; continue;
                }
                if (!event.getEventType().getName().equals(eventName)) continue;
                var stack=event.getStackTrace();
                List<String> frames=stack==null?List.of():stack.getFrames().stream()
                        .map(f -> f.getMethod().getType().getName()+"."+f.getMethod().getName()+":"+f.getLineNumber()).toList();
                String category=category(frames); total++;
                if(category.equals("product")) product++; else if(category.equals("testOnly")) test++; else other++;
                long nanos=event.getDuration().toNanos(); sum+=nanos; max=Math.max(max,nanos);
                String reason=event.hasField("pinnedReason")?String.valueOf((Object)event.getValue("pinnedReason")):"unavailable";
                String operation=event.hasField("blockingOperation")?String.valueOf((Object)event.getValue("blockingOperation")):"unavailable";
                reasons.merge(reason,1L,Long::sum); operations.merge(operation,1L,Long::sum);
                if(examples.size()<20) examples.add(Map.of("category",category,"durationNanos",nanos,"reason",reason,"blockingOperation",operation,"stack",frames));
            }
        }
        if(proofs!=1) throw new IllegalStateException("Recording lacks exactly one completed evidence marker");
        return Map.of("total",total,"product",product,"testOnly",test,"jdkOrOther",other,
                "aggregateWaitNanosAcrossThreads",sum,"maxDurationNanos",max,"reasons",reasons,"blockingOperations",operations,"examples",examples,"optionalFields",optionalFields);
    }
    public static String outcome(boolean cold, Map<String,Object> events) {
        if (!cold && ((Number)events.get("product")).longValue()!=0)
            throw new AssertionError("Steady-state product pinning: "+events);
        return cold?"DIAGNOSTIC_COMPLETE":"PASS";
    }
    public static void write(Path path, Map<String,Object> report) throws Exception {
        Files.createDirectories(path.toAbsolutePath().getParent()); Files.writeString(path,json(report)+"\n");
    }
    private static String json(Object value) {
        if(value==null) return "null";
        if(value instanceof Number || value instanceof Boolean) return value.toString();
        if(value instanceof Map<?,?> map) return "{"+String.join(",",map.entrySet().stream().map(e -> json(e.getKey().toString())+":"+json(e.getValue())).toList())+"}";
        if(value instanceof Collection<?> list) return "["+String.join(",",list.stream().map(VtEvidence::json).toList())+"]";
        StringBuilder text=new StringBuilder("\"");
        for(char c:value.toString().toCharArray()) {
            if(c=='"' || c=='\\') text.append('\\').append(c);
            else if(c<32) text.append(String.format(Locale.ROOT,"\\u%04x",(int)c));
            else text.append(c);
        }
        return text.append('"').toString();
    }
    private VtEvidence() { }
}
