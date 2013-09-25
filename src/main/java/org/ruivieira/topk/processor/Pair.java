package org.ruivieira.topk.processor;

/**
 * @author Rui Vieira
 */
public class Pair implements Comparable<Pair> {
// ------------------------------ FIELDS ------------------------------

        private final String id;
        private final long score;

// -------------------------- STATIC METHODS --------------------------

        public static Pair create(final String id, final long score) {
            return new Pair(id, score);
        }

// --------------------------- CONSTRUCTORS ---------------------------

        private Pair(String id, long score) {
            this.id = id;
            this.score = score;
        }

// --------------------- GETTER / SETTER METHODS ---------------------

        public String getId() {
            return id;
        }

        public long getScore() {
            return score;
        }

// ------------------------ CANONICAL METHODS ------------------------

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Pair pair = (Pair) o;

            if (score != pair.score) return false;
            if (!id.equals(pair.id)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + (int) (score ^ (score >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return String.format("<%s,%d>", id, score);
        }

// ------------------------ INTERFACE METHODS ------------------------


// --------------------- Interface Comparable ---------------------

        @Override
        public int compareTo(Pair other) {

            if (equals(other)) {
                return 0;
            }
            if (score < other.score) {
                return -1;
            } else if (score > other.score) {
                return 1;
            } else {
                return id.compareTo(other.getId());
            }
        }
}
