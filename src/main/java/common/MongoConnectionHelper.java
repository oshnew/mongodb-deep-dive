package common;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import java.util.Arrays;
import java.util.List;

/**
 * 몽고커넥션 헬퍼 클래스
 *
 * @author 엄승하
 */
public class MongoConnectionHelper {

	/**
	 * 단일(primary) 몽고DB 접속 MongoClient얻기
	 *
	 * @return
	 */
	public static MongoClient getNewMongoClient() {

		return new MongoClient(ConstantsMongo.dbPrimaryAddr);
	}

	/**
	 * replica모드로 몽고DB 접속 MongoClient얻기
	 *
	 * @return
	 */
	public static MongoClient getNewReplicaMongoClient() {

		List<ServerAddress> serverList = Arrays.asList(new ServerAddress(ConstantsMongo.dbPrimaryAddr, 27017), new ServerAddress(ConstantsMongo.dbSecondaryddr, 27017));
		return new MongoClient(serverList);
	}
}
