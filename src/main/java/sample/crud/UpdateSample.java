package sample.crud;

import com.mongodb.Block;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import common.ConstantsMongo;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;

/**
 *
 * @author 엄승하
 */
public class UpdateSample {

	private static JsonWriterSettings prettyPrint = JsonWriterSettings.builder().indent(true).build();
	private static MongoClient mongoClient;

	private static String dbNm = "test_log_1";
	private static String colNm = "test_update";

	public static void main(String[] args) {

		mongoClient = getNewMongoClient();
		MongoDatabase targetDB = mongoClient.getDatabase(dbNm);
		MongoCollection<Document> targetCol = targetDB.getCollection(colNm);

		targetCol.deleteMany(new Document()); //데이터 전체 삭제 후 시작(초기화)

		//테스트 데이터 insert
		targetCol.insertOne(new Document("name", "test_1").append("value_1", 1).append("value_2", 2).append("value_3", 3));

		Bson findFilter = eq("name", "test_1");

		//update 진행 전
		System.out.println("before find reuslt =>>>\n");
		System.out.println(targetCol.find(findFilter).first().toJson(prettyPrint));

		//update 후
		System.out.println("== Start update ==");

		final Document setData = new Document("$set", new Document("value_2", 99));
		targetCol.updateMany(new Document("name", "test_1"), setData);

		System.out.println("after find reuslt =>>>\n");
		System.out.println(targetCol.find(findFilter).first().toJson(prettyPrint));

		System.out.println("== End update ==");

	}

	/**
	 * 몽고DB 접속 MongoClient얻기
	 *
	 * @return
	 */
	public static MongoClient getNewMongoClient() {

		//몽고DB 접속 주소 셋팅
		List<ServerAddress> serverList = Arrays.asList(new ServerAddress(ConstantsMongo.dbPrimaryAddr, 27017));
		Block<ClusterSettings.Builder> clusterSettings = builder -> builder.hosts(serverList);

		//인증 정보 셋팅
		//몽고DB에서 use_admin 후에 아래 커맨드를 실행해서 유저 생성해둠
		// db.createUser(
		//    {
		//        user: "test-user",
		//        pwd: "test-user",
		//        roles:[
		//            {role: "readWrite", db:"test_log_1"},
		//            {role: "readWrite", db:"test_log_2"},
		//        ]
		//    }
		//)
		MongoCredential credential = MongoCredential.createScramSha1Credential(ConstantsMongo.user_name, "admin", ConstantsMongo.user_pwd.toCharArray()); //admin에서 유저 생성했음

		//커넥션 풀 셋팅
		Block<ConnectionPoolSettings.Builder> poolSetting = builder -> builder.minSize(2).maxWaitTime(4, TimeUnit.SECONDS).maxConnectionIdleTime(8,
			TimeUnit.MINUTES).maxConnectionLifeTime(20, TimeUnit.MINUTES);

		return MongoClients.create(MongoClientSettings.builder().credential(credential).applyToConnectionPoolSettings(poolSetting).applyToClusterSettings(clusterSettings).build());
	}
}
