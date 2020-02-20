// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.sequoiadb;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.Binary;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.util.encoding.StringEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class SequoiadbValueStore implements KeyColumnValueStore {

    private static final Logger log = LoggerFactory.getLogger(SequoiadbValueStore.class);

    private Sequoiadb conn;
    private SequoiadbStoreManager manager;
    private String csName;
    private String clName;

    private CollectionSpace CS = null;
    private DBCollection CL = null;
    private SequoiadbGetter entryGetter;

    private static final SequoiadbStaticBufferChange sdbStaticBufferChange = new SequoiadbStaticBufferChange();



    public SequoiadbValueStore (Sequoiadb conn,
                                String csName,
                                String clName,
                                SequoiadbStoreManager manager) {
        this.conn = conn;
        this.manager = manager;
        this.csName = csName;
        this.clName = clName;


        log.info ("SequoiadbValueStore before SequoiadbGetter, clName = " + clName);
        this.entryGetter = new SequoiadbGetter(manager.getMetaDataSchema(clName));
        log.info ("SequoiadbValueStore after SequoiadbGetter, clName = " + clName);

        initCLHandler();
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, EntryList> result = getHelper(Collections.singletonList(query.getKey()), query);

        return Iterables.getOnlyElement(result.values(), EntryList.EMPTY_LIST);
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        return getHelper(keys, query);
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, KCVMutation> mutations = ImmutableMap.of(key, new KCVMutation(additions, deletions));
        mutateMany(mutations, txh);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
//        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {

        BSONObject startObj = new BasicBSONObject();
        BSONObject endObj = new BasicBSONObject();
        BSONObject tempObj = null;
        Binary bindata = null;

        tempObj = new BasicBSONObject();
        bindata = new Binary(query.getKeyStart().as(StaticBuffer.ARRAY_FACTORY));
        tempObj.put ("$gte", bindata);
        startObj.put ("RowKey", tempObj);

        tempObj = new BasicBSONObject();
        bindata = new Binary(query.getKeyEnd().as(StaticBuffer.ARRAY_FACTORY));
        tempObj.put ("$lt", bindata);
        endObj.put ("RowKey", tempObj);

        int limit = query.getLimit();

        return getStoreKeys (startObj, endObj, limit);
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        int limit = query.getLimit();
        return getStoreKeys (null, null, limit);
    }

    @Override
    public String getName() {
        return this.csName + ":" + this.clName;
    }

    @Override
    public void close() throws BackendException {
//        releaseCLHandler ();
//        this.manager.getSequoaidbDs().releaseConnection(conn);
    }

    private KeyIterator getStoreKeys(BSONObject startObj, BSONObject endObj, int limit) {
        BSONObject selectObj = new BasicBSONObject();

        BSONObject queryObj = new BasicBSONObject();

        if (startObj != null && endObj != null) {
            BasicBSONList oList = new BasicBSONList();

            oList.add (startObj);
            oList.add (endObj);

            queryObj.put ("$and", oList);
        }

        selectObj.put ("RowKey", "ISNULL");

        DBCursor cursor = CL.query(queryObj, selectObj, null, null, 0, limit);

        return new DBCursorIterator(cursor);
    }

    private void printByte (byte[] s) {
//        try {
//            IOWriter.saveInfo("\t" + Arrays.toString(s));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    private boolean compareByteArray (byte[] source, byte[] start, byte[] end) {
        Boolean bigger_start = true;
        Boolean smaller_end = true;

        for (int i=0; i<source.length; i++) {
            byte source_b = source[i];
            if (i < start.length) {
                byte start_b = start[i];
                if (source_b < start_b) {
                    bigger_start = false;
                    break;
                }
                else if (source_b > start_b) {
                    break;
                }
            }
        }

//        if (end != null) {
//            Boolean haveBreak = false;
//            for (int i=0; i<source.length; i++) {
//                byte source_b = source[i];
//
//                if (i < end.length) {
//                    byte end_b = end[i];
//                    if (source_b > end_b) {
//                        smaller_end = false;
//                        haveBreak = true;
//                        break;
//                    }
//                    else if (source_b < end_b) {
//                        haveBreak = true;
//                        break;
//                    }
//                }
//                else {
//                    smaller_end = false;
//                }
//            }
//            if (! haveBreak) smaller_end = false;
//        }

        if (bigger_start && smaller_end) return true;

        return false;
    }

    // key in record
    private Map<StaticBuffer,EntryList> getHelper(List<StaticBuffer> keys, SliceQuery query) throws BackendException {

        final Map<StaticBuffer,EntryList> resultMap = new HashMap<StaticBuffer,EntryList>(keys.size());
//        int readNum = 0;
//        int inNum = 0;
//        int ninNum = 0;
//        int inNum2 = 0;

        Map<String, StaticBuffer> _keyMap = new HashMap<String, StaticBuffer>();

        BSONObject queryObj = new BasicBSONObject();
        BSONObject selectObj = new BasicBSONObject();
        BSONObject orderObj = new BasicBSONObject();

        BasicBSONList keyList = new BasicBSONList ();
        BasicBSONList andList = new BasicBSONList();

        BSONObject tempObj = new BasicBSONObject();
        BSONObject tempObj2 = new BasicBSONObject();

        int limit = -1;

        for (StaticBuffer key : keys) {
            String t = sdbStaticBufferChange.staticBuffer2String(key);
            Binary _key = sdbStaticBufferChange.staticBuffer2Binary (key);
            keyList.add (_key);
            _keyMap.put(t, key);
        }
        tempObj.put ("$in", keyList);
        tempObj2.put ("RowKey", tempObj);

        andList.add (tempObj2);

        BSONObject gteObj = new BasicBSONObject();
        BSONObject ltObj = new BasicBSONObject();

        byte[] colStartBytes = query.getSliceStart().length() > 0 ? query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY) : null;
        byte[] colEndBytes = query.getSliceEnd().length() > 0 ? query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY) : null;
        byte[] _colEndBytes = colEndBytes;

//        try {
//            IOWriter.saveInfo("^^^^ start key");
//            printByte (colStartBytes);
//
//            IOWriter.saveInfo("^^^^ end key");
//            printByte (colStartBytes);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

//        tempObj = new BasicBSONObject();
//        gteObj.put ("$gte", new Binary(colStartBytes));
//        tempObj.put ("Key", gteObj);
//        andList.add (tempObj);

        if (query.hasLimit()) {
            limit = query.getLimit();

//            try {
//                IOWriter.saveInfo("&&&& limit = " + limit);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        }
//        else {
//            tempObj = new BasicBSONObject();
//            ltObj.put ("$lt", new Binary(colEndBytes));
//            tempObj.put ("Key", ltObj);
//            andList.add (tempObj);
//        }

        queryObj.put ("$and", andList);

        String selectKey = sdbStaticBufferChange.staticBuffer2String(query.getSliceStart());
//        selectObj.put (selectKey,null);

        orderObj.put ("RowKey", 1);
        orderObj.put ("Key", 1);

        DBCursor cursor = this.CL.query(queryObj, selectObj, orderObj, null, 0, -1, 0);

        Binary preRowKey = null;

        Map<byte[], byte[]> mapV = new LinkedHashMap<byte[], byte[]>();

        while (cursor.hasNext()) {
//            readNum++;
            BSONObject record = cursor.getNext();

            String rowKey_str = (String) record.get("RowKey_str");
            Binary rowKey = (Binary) record.get("RowKey");

            Object _key = null;
            Object _value = null;
            _key = record.get("Key");
            _value = record.get("Value");

//            try {
//                IOWriter.saveInfo("^^^^ record key");
//                printByte (((Binary) _key).getData());
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }

            if (limit != -1) {
                _colEndBytes = null;
            }
            if (compareByteArray(((Binary) _key).getData(), colStartBytes, _colEndBytes)) {
//            if (true) {
//                inNum++;
            } else {
//                ninNum++;
                continue;
            }

            if (_key != null && _value != null) {
//                StaticBuffer key = sdbStaticBufferChange.object2StaticBuffer(_key);
//                StaticBuffer value = sdbStaticBufferChange.object2StaticBuffer(_value);

                byte[] key = ((Binary) _key).getData();
                byte[] value = ((Binary) _value).getData();

                // next rowKey
                if (preRowKey != null && !preRowKey.equals(rowKey)) {
                    // have data
                    if (mapV.size() != 0) {
//                        inNum2++;
                        resultMap.put(_keyMap.get(preRowKey), (mapV.size() == 0)
                            ? EntryList.EMPTY_LIST
                            : StaticArrayEntryList.ofBytes(mapV.entrySet(), entryGetter));
                        mapV = new LinkedHashMap<byte[], byte[]>();
                    }
                    preRowKey = rowKey;
                }
                mapV.put(key, value);
            }

            /////

//            BasicBSONList elementList = (BasicBSONList) record.get("Element");


//            for (int i=0; i<elementList.size(); ++i) {
//                BSONObject o1 = (BSONObject) elementList.get(i);
//
//                StaticBuffer key = sdbStaticBufferChange.object2StaticBuffer(o1.get("Key"));
//                StaticBuffer value = sdbStaticBufferChange.object2StaticBuffer(o1.get("Value"));
//
//                String key_ = sdbStaticBufferChange.staticBuffer2String(key);
//                Object value_ = sdbStaticBufferChange.staticBuffer2Object(value, Object.class);
//
//                mapV.put (key, value);
//            }
//            mapV.put (sdbStaticBufferChange.object2StaticBuffer("RowKey"),
//                sdbStaticBufferChange.object2StaticBuffer(rowKey));


//            resultMap.put(_keyMap.get(rowKey), (mapV.size() == 0)
//                ? EntryList.EMPTY_LIST
//                : StaticArrayEntryList.ofStaticBuffer(mapV.entrySet().iterator(), entryGetter));
        }
        cursor.close();
        resultMap.put(_keyMap.get(preRowKey), (mapV.size() == 0)
            ? EntryList.EMPTY_LIST
            : StaticArrayEntryList.ofBytes(mapV.entrySet(), entryGetter));

//        try {
//            if (!this.CL.getFullName().equals("janusgraph.systemlog")) {
//                IOWriter.saveInfo("*** " + this.CL.getFullName() + ",  recordNum = " + readNum + ", inNum = " + inNum + ", ninNum = " + ninNum + ", inNum2 = " + inNum2);
//                IOWriter.saveInfo ("### resultMap.size() = " + resultMap.size() + ", mapV.size() = " + mapV.size());
//                IOWriter.saveInfo("@@@@ queryObj = " + queryObj.toString());
//                IOWriter.saveInfo("");
//                IOWriter.saveInfo("#############################");
//                IOWriter.saveInfo("");
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        return resultMap;
    }

    // key in Array
//    private Map<StaticBuffer,EntryList> getHelper(List<StaticBuffer> keys, SliceQuery query) throws BackendException {
//
//        final Map<StaticBuffer,EntryList> resultMap = new HashMap<StaticBuffer,EntryList>(keys.size());
//
//        Map<String, StaticBuffer> _keyMap = new HashMap<String, StaticBuffer>();
//
//        BSONObject queryObj = new BasicBSONObject();
//
//        BasicBSONList keyList = new BasicBSONList ();
//
//        BSONObject tempObj = new BasicBSONObject();
//
//        for (StaticBuffer key : keys) {
//            String t = sdbStaticBufferChange.staticBuffer2String(key);
//            keyList.add (t);
//            _keyMap.put(t, key);
//        }
//        tempObj.put ("$in", keyList);
//        queryObj.put ("RowKey", tempObj);
//
//        DBCursor cursor = this.CL.query(queryObj, null, null, null);
//
//        while (cursor.hasNext()) {
//            BSONObject record = cursor.getNext();
//
//            String rowKey = (String) record.get("RowKey");
//
//            BasicBSONList elementList = (BasicBSONList) record.get("Element");
//
//            Map<StaticBuffer, StaticBuffer> mapV = new LinkedHashMap<StaticBuffer, StaticBuffer>();
//
//            for (int i=0; i<elementList.size(); ++i) {
//                BSONObject o1 = (BSONObject) elementList.get(i);
//
//                StaticBuffer key = sdbStaticBufferChange.object2StaticBuffer(o1.get("Key"));
//                StaticBuffer value = sdbStaticBufferChange.object2StaticBuffer(o1.get("Value"));
//
//                String key_ = sdbStaticBufferChange.staticBuffer2String(key);
//                Object value_ = sdbStaticBufferChange.staticBuffer2Object(value, Object.class);
//
//                mapV.put (key, value);
//            }
//            mapV.put (sdbStaticBufferChange.object2StaticBuffer("RowKey"),
//                sdbStaticBufferChange.object2StaticBuffer(rowKey));
//
//
//            resultMap.put(_keyMap.get(rowKey), (mapV.size() == 0)
//                ? EntryList.EMPTY_LIST
//                : StaticArrayEntryList.ofStaticBuffer(mapV.entrySet().iterator(), entryGetter));
//        }
//        return resultMap;
//    }

    private static void getFilter(BSONObject record, SliceQuery query) {
        byte[] colStartBytes = query.getSliceStart().length() > 0 ? query.getSliceStart().as(StaticBuffer.ARRAY_FACTORY) : null;
        byte[] colEndBytes = query.getSliceEnd().length() > 0 ? query.getSliceEnd().as(StaticBuffer.ARRAY_FACTORY) : null;

        BasicBSONList elementList = (BasicBSONList) record.get("Element");

        for (int i=0; i<elementList.size(); ++i) {
            BSONObject o1 = (BSONObject) elementList.get(i);
        }
    }

    private void initCLHandler () {
        if (!conn.isCollectionSpaceExist(csName)) { // create collection space
            try {
                Thread.sleep(10);
                if (!conn.isCollectionSpaceExist(csName)) {
                    conn.createCollectionSpace(csName);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        CS = conn.getCollectionSpace(csName);

        if (!CS.isCollectionExist(clName)) { // create collection
            try {
                Thread.sleep(50);
                if (!CS.isCollectionExist(clName)) {
                    CS.createCollection(clName);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        CL = CS.getCollection(clName);
    }

    private void checkCLIndex() {
        boolean haveIndex = false;
        if (CL == null) {
            initCLHandler();
        }
        DBCursor cursor = CL.getIndexes();
        while (cursor.hasNext()) {
            BSONObject obj = cursor.getNext();
        }
        cursor.close();

        if (! haveIndex) {
            CL.createIndex ("jKeyIndex", new BasicBSONObject("jkey", 1), true, true);
        }

    }

    private void mutateMany(Map<StaticBuffer, KCVMutation> mutations, StoreTransaction txh) throws BackendException {
        manager.mutateMany(ImmutableMap.of(clName, mutations), txh);
    }

//    public void insertOver () {
//        releaseCLHandler();
//    }

    private void releaseCLHandler () {
        CL = null;
        CS = null;
    }

    public Sequoiadb getConn () {
        return this.conn;
    }

    public void clearCl() {
        CL.truncate();
    }

    public void dropCl() {
        CS.dropCollection(this.clName);
    }

    public DBCollection getCL() {
        return this.CL;
    }

    private class DBCursorIterator implements KeyIterator {

        private final DBCursor cursor;

        private BSONObject currentRecord;
        private boolean isClosed;

        private DBCursorIterator(DBCursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            return new RecordIterator<Entry>() {
                private final Iterator<Map.Entry<byte[], byte[]>> kv;
                private final Map<byte[], byte[]> mapV = null;
                {
                    final Map<byte[], byte[]> mapV = new HashMap<byte[], byte[]>();

//                    StaticBuffer key = sdbStaticBufferChange.string2StaticBuffer("RowKey");
//                    StaticBuffer key_value = sdbStaticBufferChange.object2StaticBuffer(currentRecord.get("RowKey"));

//                    byte[] key = sdbStaticBufferChange.object2StaticBuffer("RowKey").as(StaticBuffer.ARRAY_FACTORY);
//                    byte[] key_value = ((Binary) currentRecord.get("RowKey")).getData();

                    BSONObject record = cursor.getCurrent();

                    for (String key : (Set<String>)record.toMap().keySet()) {
                        mapV.put (sdbStaticBufferChange.object2StaticBuffer(key).as(StaticBuffer.ARRAY_FACTORY),
                            ((Binary)record.get(key)).getData());
                    }

                    kv = mapV.entrySet().iterator();
                }

                @Override
                public boolean hasNext() {
                    ensureOpen();
                    return kv.hasNext();
                }

                @Override
                public Entry next() {
                    ensureOpen();
                    return StaticArrayEntry.ofBytes(kv.next(), entryGetter);
                }

                @Override
                public void close() {
                    isClosed = true;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public boolean hasNext() {
            ensureOpen();
            return cursor.hasNext();
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();

            BSONObject record = cursor.getNext();
            return StaticArrayBuffer.of (((Binary)record.get("RowKey")).getData());

//            return sdbStaticBufferChange.object2StaticBuffer(((Binary)record.get("RowKey")).getData());
        }

        @Override
        public void close() {
            cursor.close();
            isClosed = true;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void ensureOpen() {
            if (isClosed)
                throw new IllegalStateException("Iterator has been closed.");
        }
    }

    private static class SequoiadbGetter implements StaticArrayEntry.GetColVal<Map.Entry<byte[], byte[]>, byte[]> {

        private final EntryMetaData[] schema;

        private SequoiadbGetter(EntryMetaData[] schema) {
            this.schema = schema;
        }

        @Override
        public byte[] getColumn(Map.Entry<byte[], byte[]> element) {
            return element.getKey();
        }

        @Override
        public byte[] getValue(Map.Entry<byte[], byte[]> element) {
            return element.getValue();
        }

        @Override
        public EntryMetaData[] getMetaSchema(Map.Entry<byte[], byte[]> element) {
            return schema;
        }

        @Override
        public Object getMetaData(Map.Entry<byte[], byte[]> element, EntryMetaData meta) {
            switch(meta) {
                case VISIBILITY: {
//                    String key = sdbStaticBufferChange.staticBuffer2String(element.getKey());
                    String key = sdbStaticBufferChange.toStringBinary(element.getKey());
//                    String key = sdbStaticBufferChange.staticBuffer2String(element.getKey());
                    Preconditions.checkArgument(StringEncoding.isAsciiString((String) key), "check isAsciiString fail, key = %s", key);
                    return key;
                }
                default:
                    throw new UnsupportedOperationException("Unsupported meta data: " + meta);
            }
        }
    }

}
