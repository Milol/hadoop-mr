package mr;

/*import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;*/

/**
 * @ClassName TestHDFS
 * @Description:TODO
 * @Auther: liuwenshuai@100tal.com
 * @Date: 2019/8/1 16:04
 * @Version 1.0
 */
public class TestHDFS {
    /*Configuration conf = null;
    FileSystem fs = null;
    @Before
    public void conn() throws IOException {

        conf = new Configuration();
        fs = FileSystem.get(conf);
    }


    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/mytemp");
        if(fs.exists(path)) {
            fs.delete(path,true);
        }
        fs.mkdirs(path);
    }


    @Test
    public void uploadFile() throws IOException {

        //文件的上传路径
        Path path = new Path("/mytemp/haha.txt");
        FSDataOutputStream fdos = fs.create(path);

        //获取磁盘文件
        InputStream is = new BufferedInputStream(new FileInputStream("C:/Users/Administrator/Desktop/20181212_fl07.txt"));

        IOUtils.copyBytes(is, fdos, conf, true);
    }

    @Test
    public void readFile() throws IOException {
        Path path = new Path("/mytemp/log-error-2019-01-23.0.log");
        FileStatus file = fs.getFileStatus(path);
        BlockLocation[] blks = fs.getFileBlockLocations(file, 0, file.getLen());
        //遍历数组
        *//*for(BlockLocation blk : blks) {
            System.out.println(blk);
        }*//*
        FSDataInputStream fdis = fs.open(path);

        System.out.println((char)(fdis.readByte()));
        System.out.println(fdis.readByte());
        System.out.println(fdis.readByte());
    }

    @After
    public void close() throws IOException {
        fs.close();
    }*/
}
