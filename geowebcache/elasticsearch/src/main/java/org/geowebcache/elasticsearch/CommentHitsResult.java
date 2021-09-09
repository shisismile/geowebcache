package org.geowebcache.elasticsearch;

import java.util.Map;

/**
 * @author shimingen
 * @date 2021/9/9
 */
public class CommentHitsResult {
    private Integer took;
    private boolean timed_out;
    private Shard _shards;
    private OutHits hits;

    public Integer getTook() {
        return took;
    }

    public void setTook(Integer took) {
        this.took = took;
    }

    public boolean isTimed_out() {
        return timed_out;
    }

    public void setTimed_out(boolean timed_out) {
        this.timed_out = timed_out;
    }

    public Shard get_shards() {
        return _shards;
    }

    public void set_shards(Shard _shards) {
        this._shards = _shards;
    }

    public OutHits getHits() {
        return hits;
    }

    public void setHits(OutHits hits) {
        this.hits = hits;
    }

    public static class OutHits {
        private Integer total;
        private Integer max_score;
        private InnerHits[] hits;

        public Integer getTotal() {
            return total;
        }

        public void setTotal(Integer total) {
            this.total = total;
        }

        public Integer getMax_score() {
            return max_score;
        }

        public void setMax_score(Integer max_score) {
            this.max_score = max_score;
        }

        public InnerHits[] getHits() {
            return hits;
        }

        public void setHits(InnerHits[] hits) {
            this.hits = hits;
        }
    }

    public static class InnerHits {
        private String _index;
        private String _type;
        private String _id;
        private Integer _score;
        private Map<String, Object> _source;

        public String get_index() {
            return _index;
        }

        public void set_index(String _index) {
            this._index = _index;
        }

        public String get_type() {
            return _type;
        }

        public void set_type(String _type) {
            this._type = _type;
        }

        public String get_id() {
            return _id;
        }

        public void set_id(String _id) {
            this._id = _id;
        }

        public Integer get_score() {
            return _score;
        }

        public void set_score(Integer _score) {
            this._score = _score;
        }

        public Map<String, Object> get_source() {
            return _source;
        }

        public void set_source(Map<String, Object> _source) {
            this._source = _source;
        }
    }

    public static class Shard {
        private Integer total;
        private Integer successful;
        private Integer skipped;
        private Integer failed;

        public Integer getTotal() {
            return total;
        }

        public void setTotal(Integer total) {
            this.total = total;
        }

        public Integer getSuccessful() {
            return successful;
        }

        public void setSuccessful(Integer successful) {
            this.successful = successful;
        }

        public Integer getSkipped() {
            return skipped;
        }

        public void setSkipped(Integer skipped) {
            this.skipped = skipped;
        }

        public Integer getFailed() {
            return failed;
        }

        public void setFailed(Integer failed) {
            this.failed = failed;
        }
    }
}
