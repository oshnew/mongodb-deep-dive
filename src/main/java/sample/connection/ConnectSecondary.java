package sample.connection;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import common.ConstantsMongo;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Secondary 몽고DB에 인증모드로 접속 후 find쿼리 실행하는 테스트
 *
 * @author 엄승하
 */
@Slf4j
public class ConnectSecondary {

	private static MongoClient mongoClient;
	private static String colNm = "test_col";

	private static String test_db = "test_db";

	public static void main(String[] args) {

		//secondary에서 읽되 못 읽으면 primary에서 읽음
		ReadPreference readPreference = ReadPreference.secondaryPreferred(); //참고 : https://docs.mongodb.com/v4.2/core/read-preference/
		//ReadPreference readPreference = ReadPreference.primary();

		mongoClient = getMongoClient(readPreference);
		log.info("\n\ngetClusterDescription=>\n{}", mongoClient.getClusterDescription());

		MongoDatabase dbCon = mongoClient.getDatabase(test_db);

		//몽고DB 빌드정보
		Document buildInfoResults = dbCon.runCommand(new Document("buildInfo", 1));
		log.info("\n\nbuildInfo==>\n{}", buildInfoResults.toJson());

		//마스터여부인지 확인
		Document isMasterResults = dbCon.runCommand(new Document("isMaster", 1));
		// ==> secondary true로 확인됨

		log.info("\n\nisMaster==>\n{}", isMasterResults.toJson());

		excuteTest(dbCon);

		mongoClient.close(); //DB커넥션 반환
	}

	/**
	 * 테스트 실행
	 *
	 * @param mongoDB
	 */
	public static void excuteTest(MongoDatabase mongoDB) {

		System.out.println(String.format("\n\n==== Start 테스트: '%s'\n", mongoDB.getName()));

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
	 * 몽고DB 접속 MongoClient얻기
	 *
	 * @param readPreference
	 * @return
	 */
	public static MongoClient getMongoClient(ReadPreference readPreference) {

		//몽고DB 접속 주소 셋팅
		List<ServerAddress> serverList = Arrays.asList(new ServerAddress(ConstantsMongo.dbSecondaryddr, 27017));
		Block<ClusterSettings.Builder> clusterSettings = builder -> builder.hosts(serverList);

		MongoCredential credential = MongoCredential.createScramSha1Credential(ConstantsMongo.user_name, "admin", ConstantsMongo.user_pwd.toCharArray()); //admin에서 유저 생성했음

		//커넥션 풀 셋팅
		Block<ConnectionPoolSettings.Builder> poolSetting = builder -> builder.minSize(2).maxWaitTime(4, TimeUnit.SECONDS).maxConnectionIdleTime(8,
			TimeUnit.MINUTES).maxConnectionLifeTime(20, TimeUnit.MINUTES);

		return MongoClients.create(
			MongoClientSettings.builder().credential(credential).applyToConnectionPoolSettings(poolSetting).applyToClusterSettings(clusterSettings).readPreference(
				readPreference).build());
	}

}
