package sample.connection;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import common.ConstantsMongo;
import org.bson.Document;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 몽고DB 인증모드로 접속 및 기본 쿼리 수행 테스트
 *
 * @author 엄승하
 */
public class MongoDBConnectReplica {

	private static MongoClient mongoClient;
	private static String colNm = ConstantsMongo.colNm;

	public static void main(String[] args) {

		//secondary에서 읽되 못 읽으면 primary에서 읽음
		ReadPreference readPreference = ReadPreference.secondaryPreferred(); //참고 : https://docs.mongodb.com/v4.2/core/read-preference/

		mongoClient = getNewMongoClient(readPreference);
		excuteTest(mongoClient.getDatabase(ConstantsMongo.dbName));

		mongoClient.close(); //DB커넥션 반환
	}

	/**
	 * 테스트 실행
	 *
	 * @param mongoDB
	 */
	public static void excuteTest(MongoDatabase mongoDB) {

		System.out.println(String.format("\n\n==== Start 테스트: '%s'\n", mongoDB.getName()));

		mongoDB.getCollection(colNm).drop(); //삭제 후 시작
		saveDummyData(mongoDB, 1000);

		long savedCnt = mongoDB.getCollection(colNm).countDocuments();
		System.out.println(String.format("'%s' DB의 '%s'컬렉션에 저장된 row 수 ===> '%s'", mongoDB.getName(), colNm, savedCnt));

		MongoCursor<Document> cur = mongoDB.getCollection(colNm).find().limit(1).iterator();

		while (cur.hasNext()) {

			Document doc = cur.next();
			List data = new ArrayList<>(doc.values());

			System.out.println(String.format("'%s' DB의 find(일부) 결과\n'%s", mongoDB.getName(), data));
		}

		System.out.println(String.format("\n==== End 테스트: '%s'\n", mongoDB.getName()));

	}

	/**
	 * 테스트용 더미 데이터를 저장
	 *
	 * @param mongoDB
	 * @param saveCnt 저장할 더미 데이터 갯수
	 */
	public static void saveDummyData(MongoDatabase mongoDB, int saveCnt) {

		System.out.println(String.format("'%s'DB에 테스트용 더미 데이터 '%s'개를 저장", mongoDB.getName(), saveCnt));
		List<Document> docs = new ArrayList<>(saveCnt);

		Date createAt = new Date();
		String regYmdt = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

		for (int i = 1; i <= saveCnt; i++) {

			Document doc = new Document();
			doc.append("loop_cnt", i);
			doc.append("message", "dummy data save1");
			doc.append("regYmdt", regYmdt);
			doc.append("create_at", createAt);

			docs.add(doc);
		}

		mongoDB.getCollection(colNm).insertMany(docs); //N개 insert
	}

	/**
	 * 몽고DB 접속 MongoClient얻기
	 *
	 * @param readPreference
	 * @return
	 */
	public static MongoClient getNewMongoClient(ReadPreference readPreference) {

		//몽고DB 접속 주소 셋팅
		List<ServerAddress> serverList = Arrays.asList(new ServerAddress(ConstantsMongo.dbPrimaryAddr, 27017),
			new ServerAddress(ConstantsMongo.dbSecondaryddr, 27018)); //secondary는 27018포트 사용 중

		//List<ServerAddress> serverList = Arrays.asList(new ServerAddress(ConstantsMongo.dbPrimaryAddr, 27017));
		Block<ClusterSettings.Builder> clusterSettings = builder -> builder.hosts(serverList);

		MongoCredential credential = MongoCredential.createScramSha1Credential(ConstantsMongo.user_name, "admin", ConstantsMongo.user_pwd.toCharArray()); //admin에서 user defined

		//커넥션 풀 셋팅
		Block<ConnectionPoolSettings.Builder> poolSetting = builder -> builder.minSize(2).maxWaitTime(4, TimeUnit.SECONDS).maxConnectionIdleTime(8,
			TimeUnit.MINUTES).maxConnectionLifeTime(20, TimeUnit.MINUTES);

		return MongoClients.create(
			MongoClientSettings.builder().credential(credential).applyToConnectionPoolSettings(poolSetting).applyToClusterSettings(clusterSettings).readPreference(
				readPreference).build());
	}

}
