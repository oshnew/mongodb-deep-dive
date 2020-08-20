package sample.collectionManage;

import com.mongodb.Block;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.InsertManyResult;
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
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 *
 * @author 엄승하
 */
@Slf4j
public class CreateIndexSample {

	private static JsonWriterSettings prettyPrint = JsonWriterSettings.builder().indent(true).build();
	private static MongoClient mongoClient;

	private static String dbNm = "test_log_1";
	private static String colNm = "test_index";

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class MemberVO {

		private String userId;
		private String name;
		private Integer age;
		private List<String> topics;

	}

	public static void main(String[] args) {

		System.out.println("\n\n\n==== Start ====");

		mongoClient = getNewMongoClient();
		MongoDatabase targetDB = mongoClient.getDatabase(dbNm);

		targetDB.getCollection(colNm).drop(); //기존 컬렉션 drop후 실행

		//인덱스 컬렉션 추가: 참고 https://mongodb.github.io/mongo-java-driver/4.0/driver/tutorials/indexes/
		targetDB.createCollection(colNm);
		//targetDB.getCollection(colNm).createIndex(new BasicDBObject("name", 1));

		targetDB.getCollection(colNm).createIndex(Indexes.ascending("userId"), new IndexOptions().unique(true)); //유니크 인덱스
		targetDB.getCollection(colNm).createIndex(Indexes.ascending("name"));
		targetDB.getCollection(colNm).createIndex(Indexes.ascending("topics")); //멀티키 인덱스
		targetDB.getCollection(colNm).createIndex(Indexes.ascending("age", "name")); //compound 인덱스

		log.info("\n\n '{}' 컬렉션에 생성된 인덱스", colNm);

		for (Document index : targetDB.getCollection(colNm).listIndexes()) {
			System.out.println(index.toJson());
		}

		MongoCollection<MemberVO> targetCol = targetDB.getCollection(colNm, MemberVO.class);

		//테스트 데이터 insert
		MemberVO user1 = new MemberVO("id-1", "홍길동", 20, Arrays.asList("game", "music"));
		MemberVO user2 = new MemberVO("id-2", "임꺽정", 30, Arrays.asList("sports"));

		List<MemberVO> memberList = Arrays.asList(user1, user2);
		InsertManyResult resultInsertMany = targetCol.insertMany(memberList);
		log.info("{}건 insert결과. insert 성공건수:{} | insert 결과 ids:{}", memberList.size(), resultInsertMany.getInsertedIds().size(), resultInsertMany.getInsertedIds());

		Bson findFilter = eq("userId", "id-1");

		log.info("find test reuslt =>>>\n{}", targetCol.find(findFilter).first());

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
