package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created by TR on 2016/11/26.
 */
public class HdfsClient {
    /**
     * 查看hdfs中文件内容
     *
     * @param args
     * @throws Exception
     */
     public static void main(String[] args){
         HdfsClient hc = new HdfsClient();
         try{
             hc.readData(args);
         }catch (Exception e){
             e.printStackTrace();
         }
     }

    /**
     * 根据文件 url 读取文件
     * @param args
     * @throws Exception
     */
     public void readData(String[] args)  throws Exception{
         String url = "";
         if(args!=null&&args.length>0){
             url = args[0];
         }

         Configuration conf = getConfiguration();
         //得到文件系统对象
         FileSystem fs = FileSystem.get(URI.create(url),conf);
         InputStream in =null;
         try{
             in = fs.open(new Path(url));
             IOUtils.copyBytes(in,System.out,4096,false);
         }finally {
             IOUtils.closeStream(in); //关闭流
         }
     }

    /**
     * 复制本地文件 到hdfs
     * @param agrs
     * @throws Exception
     */
     public void writeData(String[] agrs) throws Exception{
         String localurl="";
         String hdfsurl = "";
         if(agrs!=null&&agrs.length>=1){
             localurl = agrs[0];
             hdfsurl = agrs[1];
         }

         InputStream in = new BufferedInputStream(new FileInputStream(localurl));
         Configuration conf = getConfiguration();
         FileSystem fs = FileSystem.get(URI.create(hdfsurl),conf);
         //创建输出流,并显示进度
         OutputStream out = fs.create(new Path(hdfsurl), new Progressable() {
             public void progress() {
                 System.out.print(".");
             }
         });

         try{
             IOUtils.copyBytes(in,out,4096,true);
         }finally {
             in.close();
             out.close();
         }
     }

     public static Configuration getConfiguration(){
         return new Configuration();
     }
}
