package sample.connection;

import com.mongodb.Block;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ClusterSettings;
import common.ConstantsMongo;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * 몽고DB 비 인증모드로 접속 및 기본 쿼리 수행 테스트
 *
 * @author 엄승하
 */
public class MongoDBConnectWithNoAuth_Ver_4 {

	private static MongoDatabase mongoDB;
	private static String colNm = ConstantsMongo.colNm;

	public static void main(String[] args) {

		MongoClient mongoClient = getNewMongoClient();
		mongoDB = mongoClient.getDatabase(ConstantsMongo.dbName);

		mongoDB.getCollection(colNm).drop(); //삭제 후 시작
		saveDummyData(1000);

		long resultCnt = mongoDB.getCollection(colNm).countDocuments();
		System.out.println(String.format("\n'%s' 컬렉션에 저장된 row 수 ===> '%s'", colNm, resultCnt));

		mongoClient.close(); //DB커넥션 반환
	}

	/**
	 * 테스트용 더미 데이터를 저장
	 *
	 * @param saveCnt 저장할 더미 데이터 갯수
	 */
	public static void saveDummyData(int saveCnt) {

		System.out.println(String.format("테스트용 더미 데이터 '%s'개를 저장", saveCnt));
		List<Document> docs = new ArrayList<>(saveCnt);

		Date createAt = new Date();

		for (int i = 1; i <= saveCnt; i++) {

			Document doc = new Document();
			doc.append("loop_cnt", i);
			doc.append("message", "dummy data save1");
			doc.append("create_at", createAt);

			docs.add(doc);
		}

		mongoDB.getCollection(colNm).insertMany(docs); //N개 insert
	}

	/**
	 * 몽고DB 접속 MongoClient얻기
	 *
	 * @return
	 */
	public static MongoClient getNewMongoClient() {

		List<ServerAddress> serverList = Arrays.asList(new ServerAddress(ConstantsMongo.dbPrimaryAddr, 27017));
		Block<ClusterSettings.Builder> clusterSettings = builder -> builder.hosts(serverList);

		//mongo와 실제 커넥션 생성(mongo client생성)
		return MongoClients.create(MongoClientSettings.builder().applyToClusterSettings(clusterSettings).build());
	}

}
