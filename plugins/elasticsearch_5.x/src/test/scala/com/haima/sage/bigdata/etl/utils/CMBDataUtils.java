package com.haima.sage.bigdata.etl.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.joda.time.DateTimeZone;

public class CMBDataUtils {

    private static final String NUMBER_OF_SHARDS = "5";

    private static final String NUMBER_OF_REPLICAS = "2";

    private static final String HZ_BATCH_PREFIX = "cmb_hzb_";

    private static final String MX_BATCH_PREFIX = "cmb_mxb_";

    private static final String XQ_BATCH_PREFIX = "cmb_xqb_";

    private static final String HZ_REALTIME_PREFIX = "cmb_hzr_";

    private static final String MX_REALTIME_PREFIX = "cmb_mxr_";

    private static final String XQ_REALTIME_PREFIX = "cmb_xqr_";

    private static final String HZ_ALIAS_PREFIX = "cmb_hz_query_";

    private static final String MX_ALIAS_PREFIX = "cmb_mx_mobile_";

    private static final String XQ_ALIAS_PREFIX = "cmb_xq_detail_";

    private static String currentDate;

    private static final String[][] HEADERS = {
            { "eac_id", "rmb_bal", "rmb_crl", "tot_out", "tot_in", "max_grp1", "max_grp2",
                    "dat_mon", "dat_dte", "db1_cod" },
            { "eac_id", "trx_nbr", "crd_nbr", "dat_flg1", "dat_flg2", "trx_cod1", "trx_cod2",
                    "ccy_cod", "trx_amt", "trx_tim", "trx_dte", "trx_des", "dtl_typ", "grp_nbr",
                    "dat_mon", "dat_dte", "db1_cod" },
            { "crd_nbr", "trx_nbr", "ent_dte", "scd_cod", "mch_nam", "cty_cod", "cit_cod",
                    "ori_ccy", "ori_amt", "bal_amt", "rcv_nam", "rcv_bak", "rcv_eac", "pay_nam",
                    "pay_bak", "pay_eac", "agn_com", "tc_cod", "dat_mon", "dat_dte", "db1_cod" } };

    private static final String BASE_PATH = "/home/whuao/cmdata/new/";

    private static final String[] ORIGINAL_FILES = { "data1.DAT", "data2.DAT", "data3.DAT" };

    private static final int SEARCH_HIT_SIZE = 20;

    private static TransportClient client = CMBDataUtils.getEsClient("es-2.3.2", "127.0.0.1",
            "9300");

    public synchronized static TransportClient getEsClient(String cluster, String ip, String port) {

        if (null == client) {
            if (cluster == null) {
                cluster = "elasticsearch";
            }
            if (ip == null) {
                ip = "localhost";
            }
            if (port == null) {
                port = "9300";
            }
            client = getNewClient(cluster, ip + ":" + port);
        }

        return client;
    }

    public synchronized static TransportClient getEsClient() {
        if (null == client) {
            String name = "elasticsearch"; // ConfigUtils.get(Constants.ES_CLUSTER_NAME);
            String address = "localhost:9300";// ConfigUtils.get(Constants.ES_CLUSTER_HOST);
            client = getNewClient(name, address);
        }
        return client;
    }

    public static TransportClient getNewClient(String name, String address) {
        Settings settings = Settings.builder().put("cluster.name", name).build();
        // client = new TransportClient(settings);
        client = new PreBuiltTransportClient(settings);
        if (address != null) {
            String[] arr = address.split(",");
            for (String tmp : arr) {
                String[] tmpArr = tmp.split(":");
                if (tmpArr.length >= 2) {
                    String host = tmpArr[0];
                    String portStr = tmpArr[1];
                    if (host != null && host.length() != 0 && portStr != null
                            && portStr.length() != 0) {
                        try {
                            int port = Integer.parseInt(portStr);
                            client.addTransportAddress(new InetSocketTransportAddress(
                                    new InetSocketAddress(host, port)));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
        return client;
    }

    public synchronized static void closeEsClient(TransportClient client) {
        if (null != client) {
            client.close();
        }
    }

    public static boolean indicesExists(String... index) {
        try {
            IndicesExistsRequest request = new IndicesExistsRequest(index);
            IndicesExistsResponse res = getEsClient().admin().indices().exists(request).actionGet();
            return res.isExists();
        } catch (Exception e) {
        }
        return false;
    }

    public static boolean deleteIndices(String... index) {
        System.out.println("deleting indices...");
        DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest(index))
                .actionGet();
        return delete.isAcknowledged();
    }

    public static boolean addAlias(String[] index, String alias) {
        IndicesAliasesResponse response = client.admin().indices().prepareAliases()
                .addAlias(index, alias).execute().actionGet();
        return response.isAcknowledged();
    }

    public static String[] initializingIndices() {

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        currentDate = formatter.format(new Date());

        String hzbIndex = HZ_BATCH_PREFIX + currentDate;
        String mxbIndex = MX_BATCH_PREFIX + currentDate;
        String xqbIndex = XQ_BATCH_PREFIX + currentDate;

        String hzrIndex = HZ_REALTIME_PREFIX + currentDate;
        String mxrIndex = MX_REALTIME_PREFIX + currentDate;
        String xqrIndex = XQ_REALTIME_PREFIX + currentDate;

        String hzAlias = HZ_ALIAS_PREFIX + currentDate;
        String mxAlias = MX_ALIAS_PREFIX + currentDate;
        String xqAlias = XQ_ALIAS_PREFIX + currentDate;

        String[] indices = { hzbIndex, mxbIndex, xqbIndex, hzrIndex, mxrIndex, xqrIndex };

        if (indicesExists(indices)) {
            deleteIndices(indices);
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        String hzType = "huizong";
        String mxType = "mingxi";
        String xqType = "xiangqing";

        try {
            XContentBuilder hzBuilder = XContentFactory.jsonBuilder().startObject()
                    .startObject(hzType).startObject("_all").field("enabled", false).endObject()
                    .startObject("_timestamp").field("enabled", "true")
                    .field("format", "yyyy-MM-dd'T'HH:mm:ss.SSSZ||epoch_millis").endObject()
                    .startObject("_routing").field("required", true).endObject()
                    .startObject("properties").startObject("eac_id").field("type", "string")
                    .field("index", "not_analyzed").field("doc_values", "true").endObject()
                    .startObject("rmb_bal").field("type", "double").field("index", "no").endObject()
                    .startObject("rmb_crl").field("type", "double").field("index", "no").endObject()
                    .startObject("tot_out").field("type", "double").field("index", "no").endObject()
                    .startObject("tot_in").field("type", "double").field("index", "no").endObject()
                    .startObject("max_grp1").field("type", "integer").field("index", "no")
                    .endObject().startObject("max_grp2").field("type", "integer")
                    .field("index", "no").endObject().startObject("dat_mon").field("type", "string")
                    .field("index", "not_analyzed").endObject().startObject("dat_dte")
                    .field("type", "date").field("format", "yyyy-MM-dd").endObject().endObject()
                    .endObject().endObject();
            XContentBuilder mxBuilder = XContentFactory.jsonBuilder().startObject()
                    .startObject(mxType).startObject("_all").field("enabled", false).endObject()
                    .startObject("_timestamp").field("enabled", "true")
                    .field("format", "yyyy-MM-dd'T'HH:mm:ss.SSSZ||epoch_millis").endObject()
                    .startObject("_routing").field("required", true).endObject()
                    .startObject("properties").startObject("eac_id").field("type", "string")
                    .field("index", "not_analyzed").field("doc_values", "true").endObject()
                    .startObject("trx_nbr").field("type", "string").field("index", "no").endObject()
                    .startObject("crd_nbr").field("type", "string").field("index", "no").endObject()
                    .startObject("dat_flg1").field("type", "string").field("index", "not_analyzed")
                    .endObject().startObject("dat_flg2").field("type", "string")
                    .field("index", "no").endObject().startObject("trx_cod1")
                    .field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("trx_cod2").field("type", "string").field("index", "not_analyzed")
                    .endObject().startObject("ccy_cod").field("type", "string").field("index", "no")
                    .endObject().startObject("trx_amt").field("type", "double").field("index", "no")
                    .endObject().startObject("trx_dte").field("type", "date")
                    .field("format", "yyyy-MM-dd").endObject().startObject("trx_tim")
                    .field("type", "date").field("format", "yyyy-MM-dd HH:mm:ss.SSSSSS").endObject()
                    .startObject("trx_des").field("type", "string").field("analyzer", "ik_max_word")
                    .field("search_analyzer", "ik_max_word").endObject().startObject("dtl_typ")
                    .field("type", "string").field("index", "no").endObject().startObject("grp_nbr")
                    .field("type", "string").field("index", "no").endObject().startObject("dat_mon")
                    .field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("dat_dte").field("type", "date").field("format", "yyyy-MM-dd")
                    .endObject().startObject("db1_cod").field("type", "string").field("index", "no")
                    .endObject().endObject().endObject().endObject();
            XContentBuilder xqBuilder = XContentFactory.jsonBuilder().startObject()
                    .startObject(xqType).startObject("_all").field("enabled", false).endObject()
                    .startObject("_timestamp").field("enabled", "true")
                    .field("format", "yyyy-MM-dd'T'HH:mm:ss.SSSZ||epoch_millis").endObject()
                    .startObject("_routing").field("required", true).endObject()
                    .startObject("properties").startObject("eac_id").field("type", "string")
                    .field("index", "not_analyzed").field("doc_values", "true").endObject()
                    .startObject("trx_nbr").field("type", "string").field("index", "no").endObject()
                    .startObject("ent_dte").field("type", "date").field("index", "no").endObject()
                    .startObject("scd_cod").field("type", "string").field("index", "not_analyzed")
                    .endObject().startObject("mch_nam").field("type", "string").field("index", "no")
                    .endObject().startObject("cty_cod").field("type", "string")
                    .field("index", "not_analyzed").endObject().startObject("cit_cod")
                    .field("type", "string").field("index", "not_analyzed").endObject()
                    .startObject("ori_ccy").field("type", "string").field("index", "no").endObject()
                    .startObject("ori_amt").field("type", "double").field("index", "no").endObject()
                    .startObject("bal_amt").field("type", "double").field("index", "no").endObject()
                    .startObject("rcv_nam").field("type", "string").field("index", "no").endObject()
                    .startObject("rcv_bak").field("type", "string").field("index", "no").endObject()
                    .startObject("rcv_eac").field("type", "string").field("index", "no").endObject()
                    .startObject("pay_nam").field("type", "string").field("index", "no").endObject()
                    .startObject("pay_bak").field("type", "string").field("index", "no").endObject()
                    .startObject("pay_eac").field("type", "string").field("index", "no").endObject()
                    .startObject("agn_com").field("type", "string").field("index", "no").endObject()
                    .startObject("tc_cod").field("type", "string").field("index", "no").endObject()
                    .startObject("dat_mon").field("type", "string").field("index", "not_analyzed")
                    .endObject().startObject("dat_dte").field("type", "date")
                    .field("format", "yyyy-MM-dd").endObject().startObject("db1_cod")
                    .field("type", "string").field("index", "no").endObject().endObject()
                    .endObject().endObject();

            XContentBuilder settingsBuilder = XContentFactory.jsonBuilder().startObject()
                    .field("number_of_shards", NUMBER_OF_SHARDS)
                    .field("number_of_replicas", NUMBER_OF_REPLICAS).endObject();

            System.out.println("\n===================\nstart creating indices at " + new Date());

            CreateIndexResponse hzbIndexResponse = client.admin().indices()
                    .create(new CreateIndexRequest(hzbIndex).settings(settingsBuilder)
                            .mapping(hzType, hzBuilder))
                    .actionGet();
            CreateIndexResponse mxbIndexResponse = client.admin().indices()
                    .create(new CreateIndexRequest(mxbIndex).settings(settingsBuilder)
                            .mapping(mxType, mxBuilder))
                    .actionGet();
            CreateIndexResponse xqbIndexResponse = client.admin().indices()
                    .create(new CreateIndexRequest(xqbIndex).settings(settingsBuilder)
                            .mapping(xqType, xqBuilder))
                    .actionGet();

            CreateIndexResponse hzrIndexResponse = client.admin().indices()
                    .create(new CreateIndexRequest(hzrIndex).settings(settingsBuilder)
                            .mapping(hzType, hzBuilder))
                    .actionGet();
            CreateIndexResponse mxrIndexResponse = client.admin().indices()
                    .create(new CreateIndexRequest(mxrIndex).settings(settingsBuilder)
                            .mapping(mxType, mxBuilder))
                    .actionGet();
            CreateIndexResponse xqrIndexResponse = client.admin().indices()
                    .create(new CreateIndexRequest(xqrIndex).settings(settingsBuilder)
                            .mapping(xqType, xqBuilder))
                    .actionGet();

            if (hzbIndexResponse.isAcknowledged() && mxbIndexResponse.isAcknowledged()
                    && xqbIndexResponse.isAcknowledged() && hzrIndexResponse.isAcknowledged()
                    && mxrIndexResponse.isAcknowledged() && xqrIndexResponse.isAcknowledged()) {
                addAlias(new String[] { hzbIndex, hzrIndex }, hzAlias);
                addAlias(new String[] { mxbIndex, mxrIndex }, mxAlias);
                addAlias(new String[] { xqbIndex, xqrIndex }, xqAlias);
            }

            System.out.println("finished creating indices at " + new Date() + "\n================");

            return indices;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;

    }

    public static void preProcessOriginalFiles() {
        for (int f = 0; f < ORIGINAL_FILES.length; f++) {
            try {
                FileInputStream fis = new FileInputStream(new File(BASE_PATH + ORIGINAL_FILES[f]));
                InputStreamReader isr = new InputStreamReader(fis, "GBK");
                BufferedReader br = new BufferedReader(isr);

                FileOutputStream fos = new FileOutputStream(new File(BASE_PATH
                        + ORIGINAL_FILES[f].substring(0, ORIGINAL_FILES[f].lastIndexOf("."))
                        + "-mod.DAT"));
                OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                BufferedWriter bw = new BufferedWriter(osw);

                String line;
                String json;
                int num = 0;
                char[] ch = { (char) 0x1c };
                while ((line = br.readLine()) != null) {
                    num++;
                    String[] set = line.replace("|", "").split("\\\\" + new String(ch));
                    StringBuffer sb = new StringBuffer();
                    sb.append(set[0]);
                    sb.append("{");
                    for (int i = 0; i < set.length; i++) {
                        sb.append("\"" + HEADERS[f][i] + "\":\"" + set[i] + "\"");
                        sb.append(",");
                    }

                    sb.delete(sb.length() - 1, sb.length());
                    sb.append("}");
                    json = sb.toString();

                    bw.write(json);
                    bw.write((char) 0x0A);
                }

                bw.flush();
                br.close();
                bw.close();
                System.out.println(num);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void importOriginalDataWithRouting(String file, String dataType)
            throws IOException {
        FileInputStream fis = new FileInputStream(new File(BASE_PATH + file));
        InputStreamReader isr = new InputStreamReader(fis, "GBK");
        BufferedReader in = new BufferedReader(isr);

        String line, json;
        int batch = 5000, total = 0;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        IndexRequestBuilder indexRequest = null;
        BulkResponse bulkResponse = null;
        Date start = new Date();
        Date end;

        int num = 0;
        String index = "";
        String type = "";
        char[] ch = { (char) 0x1c };
        String routing = null;
        while ((line = in.readLine()) != null) {
            num++;

            String[] set = line.replace("|", "").split("\\\\" + new String(ch));
            StringBuffer sb = new StringBuffer();
            routing = set[0];
            sb.append("{");
            if (file.contains("sz_crd_trx_sum")) {
                if (dataType.equalsIgnoreCase("batch")) {
                    index = HZ_BATCH_PREFIX + currentDate;
                } else {
                    index = HZ_REALTIME_PREFIX + currentDate;
                }

                type = "huizong";
                for (int i = 0; i < set.length; i++) {
                    sb.append("\"" + HEADERS[0][i] + "\":\"" + set[i] + "\"");
                    sb.append(",");
                }
            } else if (file.contains("sz_crd_trx_lst")) {
                if (dataType.equalsIgnoreCase("batch")) {
                    index = MX_BATCH_PREFIX + currentDate;
                } else {
                    index = MX_REALTIME_PREFIX + currentDate;
                }
                type = "mingxi";
                for (int i = 0; i < set.length; i++) {
                    sb.append("\"" + HEADERS[1][i] + "\":\"" + set[i] + "\"");
                    sb.append(",");
                }
            } else if (file.contains("sz_crd_trx_dtl")) {
                if (dataType.equalsIgnoreCase("batch")) {
                    index = XQ_BATCH_PREFIX + currentDate;
                } else {
                    index = XQ_REALTIME_PREFIX + currentDate;
                }
                type = "xiangqing";
                for (int i = 0; i < set.length; i++) {
                    sb.append("\"" + HEADERS[2][i] + "\":\"" + set[i] + "\"");
                    sb.append(",");
                }
            }

            sb.delete(sb.length() - 1, sb.length());
            sb.append("}");
            json = sb.toString();

            indexRequest = client.prepareIndex(index, type).setSource(json).setRouting(routing);

            bulkRequest.add(indexRequest);
            total++;

            if (total == batch) {

                bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    System.out.println(bulkResponse.buildFailureMessage());
                }
                end = new Date();
                System.out.println("========== this 5000 spend time "
                        + (end.getTime() - start.getTime()) + "\t" + end.toString() + " =====");

                start = new Date();
                bulkRequest = client.prepareBulk();
                total = 0;
            }
        }
        if (total != 0) {
            bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                System.out.println(bulkResponse.buildFailureMessage());
            }
            end = new Date();
            System.out.println("========== this end ::::: " + total + " spend         time "
                    + (end.getTime() - start.getTime()) / 1000 + "\t" + end.toString() + " =====");
        }

        System.out.print("===== total number of document is: " + num + " =====\n");
        try {
            in.close();
        } catch (IOException ex) {
        }
    }

    public static void importDataWithRouting(String index, String type, String file,
            TransportClient client, String routing) throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(file));
        String line, json;
        int idx, batch = 5000, total = 0;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        IndexRequestBuilder indexRequest = null;
        BulkResponse bulkResponse = null;
        Date start = new Date();
        Date end;

        while ((line = in.readLine()) != null) {
            idx = line.indexOf("{");
            if (idx < 0) {
                continue;
            }
            if (idx > 0) {
                routing = line.substring(0, idx);
                json = "{\"@timestamp\":\"" + (new Date()).getTime() + "\","
                        + line.substring(idx + 1);
                indexRequest = client.prepareIndex(index, type).setSource(json).setRouting(routing);
                // .setId(id)

            } else {
                json = "{\"@timestamp\":\"" + (new Date()).getTime() + "\"," + line.substring(1);
                indexRequest = client.prepareIndex(index, type).setSource(json).setRouting(routing);
            }

            bulkRequest.add(indexRequest);
            total++;

            if (total == batch) {
                bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    System.out.println(bulkResponse.buildFailureMessage());
                }
                end = new Date();
                System.out.println("========== this 5000 spend time "
                        + (end.getTime() - start.getTime()) + "\t" + end.toString() + " =====");

                start = new Date();
                bulkRequest = client.prepareBulk();
                total = 0;
            }
        }
        if (total != 0) {
            bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                System.out.println(bulkResponse.buildFailureMessage());
            }
            end = new Date();
            System.out.println("========== this end ::::: " + total + " spend time "
                    + (end.getTime() - start.getTime()) / 1000 + "\t" + end.toString() + " =====");
        }
        try {
            in.close();
        } catch (IOException ex) {
        }
    }

    public static List<String> process(String[] indices, String start, String stop,
            String dateField, String[] fields, String routing) throws IllegalArgumentException {
        if (dateField == null || dateField.isEmpty()) {
            throw new IllegalArgumentException(
                    "Invalid date field. Date field cannot be null or empty.");
        }

        DateTimeZone.setDefault(DateTimeZone.UTC);

        QueryBuilder hzQuery = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("eac_id", "10000000000025838681"))
                .filter(QueryBuilders.rangeQuery(dateField).gte(start).lte(stop));

        QueryBuilder mxQuery = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("eac_id", "10000000000025838681"))
                .must(QueryBuilders.termQuery("dat_flg1", "C"))
                .filter(QueryBuilders.rangeQuery(dateField).gte(start).lte(stop));

        SearchResponse scrollResp;

        List<String> sourceData = new ArrayList<String>();

        SearchRequestBuilder hzSearchRequest = client.prepareSearch(indices[0])
                .setIndicesOptions(IndicesOptions.fromOptions(true, false, false, false))
                /*.setQuery(hzQuery).addFields(fields).addSort(dateField, SortOrder.DESC)*/
                .setRouting(routing);

        System.out.println("hz: \n" + hzSearchRequest);

        SearchRequestBuilder mxSearchRequest = client.prepareSearch(indices[1])
                .setIndicesOptions(IndicesOptions.fromOptions(true, false, false, false))
                /*.setScroll(new TimeValue(60000)).setQuery(mxQuery).addFields(fields).setFrom(0)*/
                .setSize(SEARCH_HIT_SIZE).addSort(dateField, SortOrder.DESC).setRouting(routing);

        System.out.println("mx: \n" + mxSearchRequest);

        MultiSearchResponse msr = client.prepareMultiSearch().add(hzSearchRequest)
                .add(mxSearchRequest).execute().actionGet();

        long nbHits = 0;
        for (MultiSearchResponse.Item item : msr.getResponses()) {
            SearchResponse response = item.getResponse();
            System.out.println(response.getClass().getName());
            System.out.println(
                    "response.getHits().getTotalHits() is: " + response.getHits().getTotalHits());
            nbHits += response.getHits().getTotalHits();

            while (true) {
                for (SearchHit hit : response.getHits()) {

                    for (String field : fields) {
                        if (hit.field(field) == null) {
                            // throw new IllegalArgumentException(
                            // "Invalid field name. Field name : " + fieldName +
                            // "
                            // does
                            // not exist.");
                        } else {
                            System.out.println(
                                    "hit has field: " + field + ": " + hit.field(field).getValue());
                            if (sourceData.size() >= SEARCH_HIT_SIZE) {
                                break;
                            }
                            sourceData.add(hit.getSourceAsString());
                        }
                    }

                }

                System.out.println("response.getScrollId() is: " + response.getScrollId());

                if (response.getScrollId() != null) {
                    scrollResp = client.prepareSearchScroll(response.getScrollId())
                            .setScroll(new TimeValue(600000)).execute().actionGet();
                    // Break condition: No hits are returned
                    if (scrollResp.getHits().getHits().length == 0) {
                        System.out.println("scrollResp.getHits().getHits().length == 0");
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        System.out.println("Total hits are :" + nbHits);

        return sourceData;
    }

    public static void main(String[] args) throws IOException {

        CMBDataUtils.initializingIndices();

        String[] files = { "sz_crd_trx_sum_201605.DAT", "sz_crd_trx_lst_201605.DAT",
                "sz_crd_trx_dtl_201605.DAT" };

        for (String file : files) {
            System.out.println("\n++++++++++++++++++++\nimporting data to index at: " + new Date());
            CMBDataUtils.importOriginalDataWithRouting(file, "BATCH");
            CMBDataUtils.importOriginalDataWithRouting(file, "REALTIME");
            System.out.println(
                    "end of importing data to index at: " + new Date() + "\n+++++++++++++++++++");
        }

        String[] fields = new String[] { "eac_id", "_source" };
        List<String> result = CMBDataUtils.process(
                new String[] { "cmb_hz_query_20160602", "cmb_mx_mobile_20160602" }, "2016-04-01",
                "2016-04-30", "dat_dte", fields, "10000000000025838681");
        System.out.println(result);
    }

}
