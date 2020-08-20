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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 *
 * @author 엄승하
 */
@Slf4j
public class FindWithPojoSample {

	private static JsonWriterSettings prettyPrint = JsonWriterSettings.builder().indent(true).build();
	private static MongoClient mongoClient;

	private static String dbNm = "test_log_1";
	private static String colNm = "test_user";

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class UserVO {

		private String name;
		private Integer age;

	}

	public static void main(String[] args) {

		System.out.println("\n\n\n==== Start ====");

		mongoClient = getNewMongoClient();
		MongoDatabase targetDB = mongoClient.getDatabase(dbNm);
		MongoCollection<UserVO> targetCol = targetDB.getCollection(colNm, UserVO.class);

		targetCol.deleteMany(new Document()); //데이터 전체 삭제 후 시작(초기화)

		UserVO user = new UserVO("test_1", 10);

		//테스트 데이터 insert
		targetCol.insertOne(user);

		List<UserVO> userList = Arrays.asList(new UserVO("test_2", 20), new UserVO("test_3", 30));
		targetCol.insertMany(userList);

		Bson findFilter = eq("name", "test_1");

		//update 진행 전 find
		System.out.println("find test reuslt =>>>");
		System.out.println(targetCol.find(findFilter).first());

		//update
		System.out.println("== Start update ==");

		//final Document setData = new Document("$set", new Document("value_2", 99));
		Bson setData = set("age", 105);
		targetCol.updateMany(new Document("name", "test_1"), setData);

		System.out.println("after update find reuslt =>>>");
		System.out.println(targetCol.find(findFilter).first());

		System.out.println("== End update ==");

		System.out.println(String.format("'%s' DB의 '%s'컬렉션에 저장된 row 수 ===> '%s'", targetDB.getName(), colNm, targetCol.countDocuments()));

		System.out.println("\n\n\n==== End ====");

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
		MongoCredential credential = MongoCredential.createScramSha1Credential(ConstantsMongo.user_name, "admin", ConstantsMongo.user_pwd.toCharArray()); //admin에서 유저 생성했음

		//커넥션 풀 셋팅
		Block<ConnectionPoolSettings.Builder> poolSetting = builder -> builder.minSize(2).maxWaitTime(4, TimeUnit.SECONDS).maxConnectionIdleTime(8,
			TimeUnit.MINUTES).maxConnectionLifeTime(20, TimeUnit.MINUTES);

		//pojo 사용을 위한 설정
		//참고: https://mongodb.github.io/mongo-java-driver/4.0/driver/getting-started/quick-start-pojo/
		CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), fromProviders(PojoCodecProvider.builder().automatic(true).build()));

		return MongoClients.create(
			MongoClientSettings.builder().credential(credential).codecRegistry(pojoCodecRegistry).applyToConnectionPoolSettings(poolSetting).applyToClusterSettings(
				clusterSettings).build());
	}
}
