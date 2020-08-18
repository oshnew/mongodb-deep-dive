package common;

/**
 *
 * @author 엄승하
 */
public class ConstantsMongo {

	public static String dbPrimaryAddr = "192.168.56.1"; //primary 몽고DB 주소
	public static String dbSecondaryddr = "192.168.56.1"; //secondary 몽고DB 주소(replica 모드로 접속시 필요

	public static String dbName = "test_mongodb"; //테스트 DB명
	public static String colNm = "test_col"; //테스트 컬렉션명
}
