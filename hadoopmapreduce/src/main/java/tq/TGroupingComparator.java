package tq;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ClassName TGroupingComparator
 * @Description:TODO
 * @Auther: liuwenshuai@100tal.com
 * @Date: 2019/8/5 17:04
 * @Version 1.0
 */
public class TGroupingComparator extends WritableComparator {
    Tq t1 = null;
    Tq t2 = null;

    public TGroupingComparator() {
        super(Tq.class, true );
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        t1 = (Tq) a;
        t2 = (Tq) b;

        int c1 = Integer.compare(t1.getYear(), t2.getYear());
        if (c1 == 0 ) {
            return Integer.compare(t1.getMonth(), t2.getMonth());
        }
        return c1;
    }
}
