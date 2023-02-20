package base.flink.connectors.es;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.Serializable;
import java.util.Map;

public abstract class RichEsSinkFunc<T> implements ElasticsearchSinkFunction<T>,Serializable{

    private String index;
    private String docType;
    private String idFeild;
    private DocWriteRequest.OpType opType = DocWriteRequest.OpType.INDEX;

    private static final long serialVersionUID = 1L;

    public RichEsSinkFunc<T> setIndex(String index) {
        this.index = index;
        return this;
    }

    public RichEsSinkFunc<T> setDocType(String docType) {
        this.docType = docType;
        return this;
    }

    public RichEsSinkFunc<T> setIdFeild(String idFeild) {
        this.idFeild = idFeild;
        return this;
    }

    public RichEsSinkFunc<T> setOpType(DocWriteRequest.OpType opType) {
        this.opType = opType;
        return this;
    }

    public abstract Map<String, Object> createSource(T element);

    @Override
    public void process(T element, RuntimeContext ctx, RequestIndexer indexer) {
        Map<String, Object> data = createSource(element);

        if (this.opType != null && (this.opType.equals(DocWriteRequest.OpType.INDEX) || this.opType.equals(DocWriteRequest.OpType.CREATE))) {
            IndexRequest indexRequest = Requests.indexRequest()
                    .index(index)
                    .source(data, XContentType.JSON);

            if (this.docType != null) {
                indexRequest.type(this.docType);
            }
            if (this.idFeild != null) {
                indexRequest.id(data.get(this.idFeild).toString());

            }

            indexRequest.opType(this.opType);

            indexer.add(indexRequest);

        } else {
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.doc(data, XContentType.JSON).index(index).
                    docAsUpsert(true);
            if (this.idFeild != null) {
                updateRequest.id(data.get(this.idFeild).toString());
            }
            if (this.docType != null) {
                updateRequest.type(this.docType);
            }

            indexer.add(updateRequest);

        }
    }
}
