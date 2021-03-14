import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

public class CompareDB2 {
	final static Logger log = Logger.getLogger(CompareDB2.class);

	public static void main(String[] args) {
		List<String> schemas = new ArrayList<>();
		BufferedWriter bw = null;
		SimpleDateFormat df = new SimpleDateFormat("_dd_MM_YYYY_HH_mm_SS__" + new Random().nextInt(10));
		String fileExt = df.format(new Date());
		try {
			log.info("Start Processing DB Extract");
			ReadMetaData data = new ReadMetaData();
			bw = new BufferedWriter(new FileWriter(new File("data/Schemas" + fileExt + ".txt")));
			schemas = data.getSchemaData(bw);
			bw.close();
			for (int i = 0; i < schemas.size(); i++) {
				try {
					bw = new BufferedWriter(new FileWriter(new File("data/" + schemas.get(i) + fileExt + ".txt")));
					data.processMetaData(schemas.get(i), bw);
				} catch (Exception e) {
					log.error(("Error In Processing : " + e.getClass() + ": " + e.getMessage() + ": " + e.getCause()),
							e);
					System.out.println(e.getMessage());
				} finally {
					try {
						if (bw != null)
							bw.close();
					} catch (Exception e) {
					}
				}
			}
			log.info("End Processing DB Extract");
		} catch (Exception e) {
			log.error(("Error In Processing : " + e.getClass() + ": " + e.getMessage() + ": " + e.getCause()), e);
			System.out.println(e.getMessage());
		} finally {
			try {
				if (bw != null)
					bw.close();
			} catch (IOException e) {
				log.error("Error In Finally Closing : " + e);
			}
		}
	}

}
