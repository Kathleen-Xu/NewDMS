package miniSQL;

import miniSQL.CATALOGMANAGER.CatalogManager;
import miniSQL.CATALOGMANAGER.attribute;
import miniSQL.CATALOGMANAGER.index;
import miniSQL.CATALOGMANAGER.table;
import miniSQL.RECORDMANAGER.conditionNode;
import miniSQL.RECORDMANAGER.tuple;
import miniSQL.lexer.Comparison;
import miniSQL.lexer.Lexer;
import miniSQL.lexer.Tag;
import miniSQL.lexer.Token;

import java.io.*;
import java.util.Vector;

/*
 * 表名只能是字母开头的东西
 */
public class Interpreter {
   private static Token thetoken;//记录当前token
   private static boolean isSynCorrect=true;
   private static boolean isSemaCorrect=true;
   private static String synErrMsg;
   private static String semaErrMsg;
   
   
   public static void main(String []args){		   
	   System.out.println("Welcome to miniSQL.Please enter the command");
	   
	   try{
		    API.Initialize();
			while (true) {
				BufferedReader reader=new BufferedReader(new InputStreamReader(System.in));
				Parsing(reader);
			}
        }
       catch(Exception e){
    	   System.out.println("Interpreter error:"+e.getMessage());
    	   e.printStackTrace();
       }
   }
   //逐条语句进行解析
   public static String Parsing(BufferedReader reader)throws IOException {
	   isSynCorrect = true;
	   isSemaCorrect = true;
	   Lexer lexer = new Lexer(reader);
	   while(lexer.getReaderState() == false){
		   //System.out.print("*");
		   if(!isSynCorrect){   //调试通过后再加
			   if(thetoken.toString().equals(";")){
				   System.out.println(synErrMsg);
				   isSemaCorrect=true;
				   isSynCorrect=true;
				   continue;
			   }
		   }
		   thetoken=lexer.scan();
		   if(thetoken.tag==Tag.EXECFILE){
			   System.out.println("execfile!");
			   thetoken=lexer.scan();
			   // System.out.println(thetoken.toString());
			   File file=new File(thetoken.toString()+".txt");
			   thetoken=lexer.scan();
			   if(thetoken.toString().equals(";")){
				   if(file.exists()){
					   System.out.println("file exists");
					   BufferedReader reader2=new BufferedReader(new FileReader(file));
					   Parsing(reader2);
					   isSynCorrect=true;//为了奇怪的bug
					   continue;
				   }
				   else{
					   System.out.println("file doesn't exist");
					   synErrMsg="The file "+file.getName()+" doesn't exist";
					   isSynCorrect=false;
					   continue;
				   }
			   }
			   else{
				   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
				   continue;
			   }
		   }
		   else if(thetoken.tag==Tag.QUIT){
			   System.out.println("quit!");
			   thetoken=lexer.scan();
			   if(thetoken.toString().equals(";")){
				   System.out.println("Quit the MiniSql. See you next time!");
				   API.close();
				   reader.close();
//					 System.exit(0);
				   return "Quit the MiniSql. See you next time!";
			   }
			   else{
				   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
				   continue;
			   }

		   }
		   else if(thetoken.tag==Tag.CREATE){
			   System.out.println("create!");
			   thetoken=lexer.scan();
			   /*
				* create table 语义错误种类
				* 1 table name已存在
				* 2 primary key不存在
				* 3 重复attribute属性
				* 4 char(n) 的n越界
				*/
			   if(thetoken.tag==Tag.TABLE){
				   thetoken=lexer.scan();
				   if(thetoken.tag==Tag.ID){	//create table 表名
					   String tmpTableName=thetoken.toString();
					   Vector<attribute>tmpAttributes=new Vector<attribute>();
					   String tmpPrimaryKey=null;
					   if(CatalogManager.isTableExist(tmpTableName)){
						   semaErrMsg="The table "+tmpTableName+" already exists";
						   isSemaCorrect=false;
					   }
					   thetoken=lexer.scan();
					   if(thetoken.toString().equals("(")){//create table 表名(
						   thetoken=lexer.scan();
						   while(!thetoken.toString().equals(")")&&!thetoken.toString().equals(";")){
							   if(thetoken.tag==Tag.ID){ //create table 表名 ( 属性名
								   String tmpAttriName=thetoken.toString();
								   String tmpType;
								   int tmpLength;
								   boolean tmpIsU=false;
								   if(CatalogManager.isAttributeExist(tmpAttributes, tmpAttriName)){
									   semaErrMsg="Duplicated attribute names "+tmpAttriName;
									   isSemaCorrect=false;
								   }
								   thetoken=lexer.scan();
								   if(thetoken.tag==Tag.TYPE){//create table 表名 ( 属性名 类型名
									   tmpType=thetoken.toString();
									   if(tmpType.equals("char")){//针对char(n)类型做特殊处理
										   thetoken=lexer.scan();
										   if(thetoken.toString().equals("(")){
											   thetoken=lexer.scan();
											   if(thetoken.tag==Tag.INTNUM){
												   tmpLength = Integer.parseInt(thetoken.toString());
												   if(tmpLength<1||tmpLength>255){
													   semaErrMsg="The length of char should be 1<=n<=255";
													   isSemaCorrect=false;
												   }
												   thetoken=lexer.scan();
												   if(thetoken.toString().equals(")"));
												   else{
													   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
													   break;
												   }
											   }
											   else{
												   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
												   break;
											   }
										   }
										   else{
											   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
											   break;
										   }

									   }
									   else{//不是char
										   tmpLength=4;

									   }
									   thetoken=lexer.scan();
									   if(thetoken.tag==Tag.UNIQUE){
										   tmpIsU=true;
										   thetoken=lexer.scan();
									   }
									   else ;
									   if(thetoken.toString().equals(",")){
										   tmpAttributes.addElement(new attribute(tmpAttriName,tmpType,tmpLength,tmpIsU));
									   }
									   else if(thetoken.toString().equals(")")){
										   tmpAttributes.addElement(new attribute(tmpAttriName,tmpType,tmpLength,tmpIsU));
										   break;
									   }
									   else{
										   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
										   break;
									   }
								   }
								   else{
									   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
									   break;
								   }
							   }

							   else if(thetoken.tag==Tag.PRIMARY){
								   thetoken=lexer.scan();
								   if(thetoken.tag==Tag.KEY){
									   thetoken=lexer.scan();
									   if(thetoken.toString().equals("(")){
										   thetoken=lexer.scan();
										   if(thetoken.tag==Tag.ID){
											   tmpPrimaryKey=thetoken.toString();
											   thetoken=lexer.scan();
											   if(thetoken.toString().equals(")")){
												   if(!CatalogManager.isAttributeExist(tmpAttributes, tmpPrimaryKey)){
													   semaErrMsg="The attribute "+tmpPrimaryKey+" doesn't exist";isSemaCorrect=false;
												   }
											   }
											   else{

												   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
												   break;
											   }
										   }
										   else{
											   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
											   break;
										   }
									   }
									   else{
										   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
										   break;
									   }
								   }
								   else{

									   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
									   break;
								   }
							   }

							   else{
								   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
								   break;
							   }
							   thetoken=lexer.scan();
						   }//end of while")"
						   thetoken=lexer.scan();

						   if(isSynCorrect&&thetoken.toString().equals(";")){
							   /*
								* 执行create table 操作
								* */
							   if(tmpPrimaryKey==null){
								   synErrMsg="Synthetic error: no primary key defined";isSynCorrect=false;
								   continue;
							   }
							   if(isSemaCorrect){
								   if(API.createTable(tmpTableName,new table(tmpTableName,tmpAttributes,tmpPrimaryKey))) {
									   System.out.println("create table "+tmpTableName+" succeeded");
									   return "create table "+tmpTableName+" succeeded";
								   }
								   else {
									   System.out.println("Error: create table failed");
									   return "Error: create table failed";
								   }

							   }
							   else{
								   System.out.print(semaErrMsg);
								   System.out.println(", create table "+tmpTableName+" failed");
								   isSemaCorrect=true;
								   return semaErrMsg + ", create table "+tmpTableName+" failed";
							   }
//								 continue;
						   }
						   else{
							   //System.out.println("stop here"+isSynCorrect);
							   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
							   continue;
						   }
					   }
				   }
				   else{
					   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
					   continue;
				   }
			   }
			   /*
				* create index 语义错误种类
				* 1 index name已存在
				* 2 table name 不存在
				* 3 attribute不存在
				* 4 attribute已经是索引
				* 5 attribute 不是unique
				*/
			   else if(thetoken.tag==Tag.INDEX){
				   String tmpIndexName,tmpTableName,tmpAttriName;
				   thetoken=lexer.scan();
				   if(thetoken.tag==Tag.ID){//create index a
					   tmpIndexName=thetoken.toString();
					   if(CatalogManager.isIndexExist(tmpIndexName)){
						   semaErrMsg="The index "+tmpIndexName+" already exist";
						   isSemaCorrect=false;
					   }
					   thetoken=lexer.scan();
					   if(thetoken.tag==Tag.ON){//create index a on
						   thetoken=lexer.scan();
						   if(thetoken.tag==Tag.ID){//create index a on b
							   tmpTableName=thetoken.toString();
							   if(!CatalogManager.isTableExist(tmpTableName)){
								   semaErrMsg="The table "+tmpTableName+" doesn't exist";
								   isSemaCorrect=false;
							   }
							   thetoken=lexer.scan();
							   if(thetoken.toString().equals("(")){
								   thetoken=lexer.scan();
								   if(thetoken.tag==Tag.ID){
									   tmpAttriName=thetoken.toString();
									   if(isSemaCorrect&&!CatalogManager.isAttributeExist(tmpTableName, tmpAttriName)){
										   semaErrMsg="The attribute "+tmpAttriName+" doesn't exist on "+tmpTableName;
										   isSemaCorrect=false;
									   }
									   else if(isSemaCorrect&&!CatalogManager.inUniqueKey(tmpTableName, tmpAttriName)){
										   semaErrMsg="The attribute "+tmpAttriName+" on "+tmpTableName+" is not unique";
										   isSemaCorrect=false;
									   }
									   else if(isSemaCorrect&&CatalogManager.isIndexKey(tmpTableName, tmpAttriName)){
										   semaErrMsg="The attribute "+tmpAttriName+" on "+tmpTableName+" is already an index";
										   isSemaCorrect=false;
									   }
									   thetoken=lexer.scan();
									   if(thetoken.toString().equals(")")&&lexer.scan().toString().equals(";")){//create index a on b;
										   /*
											* 执行create index操作
											* */
										   if(isSemaCorrect){
											   if(API.createIndex(new index(tmpIndexName,tmpTableName,tmpAttriName))) {
												   System.out.println("create index "+tmpIndexName+" on "+tmpTableName+" ("+tmpAttriName+") succeeded.");
												   return "create index "+tmpIndexName+" on "+tmpTableName+" ("+tmpAttriName+") succeeded.";
											   }
											   else {
												   System.out.println("Error: create index failed");
												   return "Error: create index failed";
											   }
										   }
										   else{
											   System.out.print(semaErrMsg);
											   System.out.println(", create index failed");
											   isSemaCorrect=true;
											   return semaErrMsg + ", create index failed";
										   }
									   }
									   else{
										   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
										   continue;
									   }
								   }
								   else{
									   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
									   continue;
								   }

							   }
							   else{
								   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
								   continue;
							   }
						   }
						   else{
							   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
							   continue;
						   }
					   }
					   else{
						   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
						   continue;
					   }
				   }
				   else{
					   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
					   continue;
				   }
			   }
			   else{
				   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
				   continue;
			   }
		   }//end of create
		   else if(thetoken.tag==Tag.DROP){
			   System.out.println("drop!");
			   thetoken=lexer.scan();
			   /*
				* drop table 语义错误种类  1该table不存在
				*/
			   if(thetoken.tag==Tag.TABLE){
				   String tmpTableName;
				   thetoken=lexer.scan();
				   if(thetoken.tag==Tag.ID){//drop table a
					   tmpTableName=thetoken.toString();
					   if(!CatalogManager.isTableExist(tmpTableName)){
						   semaErrMsg="The table "+tmpTableName+" doesn't exist, ";
						   isSemaCorrect=false;
					   }
					   thetoken=lexer.scan();
					   if(thetoken.toString().equals(";")){//drop table a ;
						   /*
							* 执行drop table
							* 操作*/
						   if(isSemaCorrect){
							   if(API.dropTable(tmpTableName)) {
								   System.out.println("drop table "+tmpTableName+" succeeded");
								   return "drop table "+tmpTableName+" succeeded";
							   }
						   }
						   else{
							   System.out.print(semaErrMsg);
							   System.out.println("drop table "+tmpTableName+" failed");
							   isSemaCorrect=true;
							   return semaErrMsg + "drop table "+tmpTableName+" failed";
						   }
						   continue;
					   }
					   else{
						   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
						   continue;
					   }

				   }
				   else{
					   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
					   continue;
				   }
			   }//end of drop table
			   /*
				* drop index 语义错误种类
				* 1该index不存在
				* 2 该index是主键
				*/
			   else if(thetoken.tag==Tag.INDEX){//drop index
				   thetoken=lexer.scan();
				   if(thetoken.tag==Tag.ID){//drop index a
					   String tmpIndexName=thetoken.toString();
					   if(!CatalogManager.isIndexExist(tmpIndexName)){
						   semaErrMsg="The index "+tmpIndexName+" doesn't exist, ";
						   isSemaCorrect=false;
					   }
					   if(tmpIndexName.endsWith("_prikey")){
						   semaErrMsg="The index "+tmpIndexName+" is a primary key, ";
						   isSemaCorrect=false;
					   }
					   thetoken=lexer.scan();
					   if(thetoken.toString().equals(";")){//drop index a ;
						   /*
							* 执行drop index 操作
							* */
						   if(isSemaCorrect){
							   if(API.dropIndex(tmpIndexName)) {
								   System.out.println("drop index "+tmpIndexName+" succeeded");
								   return "drop index "+tmpIndexName+" succeeded";
							   }
						   }
						   else{
							   System.out.print(semaErrMsg);
							   System.out.println("drop index "+tmpIndexName+" failed");
							   isSemaCorrect=true;
							   return semaErrMsg + "drop index "+tmpIndexName+" failed";
						   }
						   continue;
					   }
					   else{
						   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
						   continue;
					   }
				   }
				   else{
					   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
					   continue;
				   }
			   }
			   else{
				   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
				   continue;
			   }
		   }//end of drop
		   /*
			* insert into 语义错误种类
			* 1 table 不存在
			* 2 插入的tuple数量不对
			* 3 插入的tuple类型（及长度）不对
			* 4 unique key 有重复插入（未实现,需要record manager配合）
			*/
		   else if(thetoken.tag==Tag.INSERT){
			   System.out.println("insert!");
			   thetoken=lexer.scan();
			   if(thetoken.tag==Tag.INTO){//insert into
				   thetoken=lexer.scan();
				   if(thetoken.tag==Tag.ID){//insert into 表名
					   String tmpTableName=thetoken.toString();
					   Vector<String>units=new Vector<String>();
					   if(!CatalogManager.isTableExist(tmpTableName)){
						   semaErrMsg="The table "+tmpTableName+" doesn't exist";
						   isSemaCorrect=false;
					   }

					   thetoken=lexer.scan();
					   if(thetoken.tag==Tag.VALUES){
						   thetoken=lexer.scan();
						   if(thetoken.toString().equals("(")){
							   thetoken=lexer.scan();
							   String tmpValue ;
							   int i=0;//记录unit的index
							   while(!thetoken.toString().equals(")")){	//insert into 表名 values()
								   // System.out.println(thetoken.tag);
								   if(isSemaCorrect&&i>=CatalogManager.getTableAttriNum(tmpTableName)){
									   isSemaCorrect=false;
									   semaErrMsg="The number of values is larger than that of attributes";
								   }
								   else if(isSemaCorrect){

									   tmpValue=thetoken.toString();
									   int tmpLength=CatalogManager.getLength(tmpTableName, i);
									   String tmpType=CatalogManager.getType(tmpTableName, i);
									   String tmpAttriName=CatalogManager.getAttriName(tmpTableName, i);

									   if(CatalogManager.inUniqueKey(tmpTableName, tmpAttriName)){//对于unique key的判别
										   conditionNode tmpCondition=new conditionNode(tmpAttriName,"=",thetoken.toString());

										   if(isSemaCorrect&&API.selectTuples(tmpTableName,null,tmpCondition).size()!=0){
											   isSemaCorrect=false;
											   semaErrMsg="The value "+thetoken.toString()+" already exists in the unique attrubute "+tmpAttriName;
										   }
									   }
									   if(thetoken.tag==Tag.STR){//字符类型

										   //if(tmpType.equals("char"))tmpLength/=2;
										   if(!tmpType.equals("char")
												   ||tmpLength<tmpValue.getBytes().length){
											   isSemaCorrect=false;
											   semaErrMsg="The type of value +"+tmpValue+" should be "+tmpType+"("+tmpLength+"), not char("+tmpValue.getBytes().length+")";
										   }
										   i++;
										   units.add(tmpValue);
									   }
									   else if(thetoken.tag==Tag.INTNUM){//整型

										   if(!tmpType.toString().equals("int")
												   &&!tmpType.equals("float")){
											   isSemaCorrect=false;
											   semaErrMsg="The type of value +"+tmpValue+" should be "+tmpType+"("+tmpLength+"), not be int";
										   }
										   i++;
										   units.add(tmpValue);
									   }
									   else if(thetoken.tag==Tag.FLOATNUM){//浮点型

										   if(!CatalogManager.getType(tmpTableName, i++).equals("float")){
											   isSemaCorrect=false;
											   semaErrMsg="The type of value +"+tmpValue+" should be "+tmpType+"("+tmpLength+"), not float";
										   }
										   units.add(tmpValue);
									   }
									   else{
										   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
										   break;
									   }
								   }
								   thetoken=lexer.scan();
								   if(thetoken.toString().equals(","))thetoken=lexer.scan();
								   else if(thetoken.toString().equals(")"));
								   else{
									   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
									   break;
								   }
							   }
							   if(isSemaCorrect&&i<CatalogManager.getTableAttriNum(tmpTableName)){
								   isSemaCorrect=false;
								   semaErrMsg="The number of values is smaller than that of attributes";
							   }
							   thetoken=lexer.scan();
							   if(isSynCorrect&&thetoken.toString().equals(";")){
								   /*
									* 执行insert 操作
									* */


								   if(isSemaCorrect){
									   if(API.insertTuples(tmpTableName,new tuple(units))) {
										   System.out.println("insert into "+tmpTableName+" succeeded");
										   return "insert into "+tmpTableName+" succeeded";
									   }
									   else {
										   System.out.println("Error: insert into "+tmpTableName+" failed");
										   return "Error: insert into "+tmpTableName+" failed";
									   }
								   }
								   else{
									   System.out.print(semaErrMsg);
									   System.out.println(", insert failed");
									   isSemaCorrect=true;
									   return semaErrMsg + ", insert failed";
								   }
									/* for(int i=0;i<tmpValue.size();i++){

										 System.out.print(tmpValue.get(i)+"("+tmpType.get(i).toString()+")");

									 }*/

							   }
							   else{
								   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
								   continue;
							   }
						   }
						   else{
							   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
							   continue;
						   }
					   }
					   else{
						   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
						   continue;
					   }
				   }
				   else{
					   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
					   continue;
				   }
			   }
			   else{
				   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
				   continue;
			   }
		   }//end of insert
		   /*
			* delete 语义错误种类
			* 1 table 不存在
			* 2 where 条件有误 见parsingCondition
			*/
		   else if(thetoken.tag==Tag.DELETE){
			   System.out.println("delete!");
			   thetoken=lexer.scan();
			   if(thetoken.tag==Tag.FROM){//delete from
				   thetoken=lexer.scan();
				   if(thetoken.tag==Tag.ID){
					   String tmpTableName=thetoken.toString();
					   if(!CatalogManager.isTableExist(tmpTableName)){
						   semaErrMsg="The table "+tmpTableName+" doesn't exist";
						   isSemaCorrect=false;
					   }
					   thetoken=lexer.scan();
					   if(thetoken.tag==Tag.WHERE){//delete from 表名 where 条件；
						   // 添加搜索条件
						   conditionNode tmpConditionNodes=ParsingCondition(lexer,tmpTableName,";");
						   if(thetoken.toString().equals(";")){//delete from 表名；

							   if(isSemaCorrect&&isSynCorrect){
								   /*
									* 执行delete where 操作
									*/
								   int deleteNum=API.deleteTuples(tmpTableName, tmpConditionNodes);
								   System.out.println("delete "+deleteNum+ " tuples from table "+tmpTableName);
								   return "delete "+deleteNum+ " tuples from table "+tmpTableName;
								   //System.out.println("delete succeeded");
							   }
							   else if(!isSynCorrect){
								   continue;
							   }
							   else{
								   System.out.println(semaErrMsg+", delete tuples failed");
								   isSemaCorrect=true;
								   return semaErrMsg + ", delete tuples failed";
							   }
						   }
						   else{
							   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
							   continue;
						   }
					   }
					   else if(thetoken.toString().equals(";")){//delete from 表名；

						   if(isSemaCorrect){
							   /*
								* 执行delete操作
								*/
							   int deleteNum=API.deleteTuples(tmpTableName, null);

							   System.out.println("delete "+deleteNum+ " tuples from table "+tmpTableName);
							   return "delete "+deleteNum+ " tuples from table "+tmpTableName;

						   }
						   else{
							   System.out.println(semaErrMsg+", delete tuples failed");
							   isSemaCorrect=true;
							   return semaErrMsg+", delete tuples failed";
						   }
					   }
					   else{

						   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
						   continue;
					   }
				   }
				   else{
					   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
					   continue;
				   }
			   }
			   else{
				   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
				   continue;
			   }
		   }
		   /*
			* select 语义错误种类
			* 1 table 不存在
			* 2 where 条件有误  见parsingCondition
			*/
		   else if(thetoken.tag==Tag.SELECT){
			   System.out.println("select!");
			   Vector<String>tmpAttriNames=ParsingProjection(lexer);
			   if(isSynCorrect&&thetoken.tag==Tag.FROM){//select * from
				   thetoken=lexer.scan();
				   if(thetoken.tag==Tag.ID){
					   String tmpTableName=thetoken.toString();
					   String tmpTableName2="";
					   boolean joinflag=false;
					   if(isSemaCorrect&&!CatalogManager.isTableExist(tmpTableName)){
						   semaErrMsg="The table "+tmpTableName+" doesn't exist";
						   isSemaCorrect=false;
					   }
					   if(tmpAttriNames!=null)//对于投影的属性进行判断
						   for(int i=0;i<tmpAttriNames.size();i++){
							   if(isSemaCorrect&&!CatalogManager.isAttributeExist(tmpTableName, tmpAttriNames.get(i))){
								   semaErrMsg="The attribute "+tmpAttriNames.get(i)+" doesn't exist";
								   isSemaCorrect=false;
							   }
						   }
					   thetoken=lexer.scan();
					   //如果有join
					   if(thetoken.tag==Tag.JOIN||thetoken.toString().equals(",")){
						   joinflag=true;
						   thetoken=lexer.scan();
						   if(thetoken.tag==Tag.ID){

							   tmpTableName2=thetoken.toString();
							   if(isSemaCorrect&&!CatalogManager.isTableExist(tmpTableName2)){
								   semaErrMsg="The table "+tmpTableName2+" doesn't exist";
								   isSemaCorrect=false;
							   }
							   thetoken=lexer.scan();
						   }
						   else{
							   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
							   continue;
						   }
					   }
					   if(isSynCorrect&&thetoken.tag==Tag.WHERE){//select * from 表名 where 条件；
						   /* 添加搜索条件*/

						   if(joinflag){
							   thetoken=lexer.scan();
							   String[]tmpName1=new String[2],tmpName2=new String[2];

							   if(thetoken.tag==Tag.ID){
								   tmpName1=thetoken.toString().split("\\.");
								   if(isSemaCorrect&&!CatalogManager.isTableExist(tmpName1[0])){
									   semaErrMsg="The table "+tmpName1[0]+" doesn't exist";
									   isSemaCorrect=false;
								   }
								   if(isSemaCorrect&&!CatalogManager.isAttributeExist(tmpTableName, tmpName1[1])){
									   semaErrMsg="The attribute "+tmpName1[1]+" doesn't exist";
									   isSemaCorrect=false;
								   }
								   thetoken=lexer.scan();
								   //if(thetoken.toString().equals("=")){
								   if(thetoken.tag==Tag.OP){
									   thetoken=lexer.scan();
									   if(thetoken.tag==Tag.ID){
										   tmpName2=thetoken.toString().split("\\.");
										   if(isSemaCorrect&&!CatalogManager.isTableExist(tmpName2[0])){
											   semaErrMsg="The table "+tmpName2[0]+" doesn't exist";
											   isSemaCorrect=false;
										   }
										   if(isSemaCorrect&&!CatalogManager.isAttributeExist(tmpTableName, tmpName2[1])){
											   semaErrMsg="The attribute "+tmpName2[1]+" doesn't exist";
											   isSemaCorrect=false;
										   }
										   thetoken=lexer.scan();
										   if(thetoken.toString().equals(";")){
											   if(isSemaCorrect&&isSynCorrect){
												   /*
													* 执行select join 操作*/
												   String selectReturn = "";
												   for(int i=0;i<CatalogManager.getTableAttriNum(tmpTableName);i++){ //输出属性名
													   System.out.print("\t"+CatalogManager.getAttriName(tmpTableName, i));
													   selectReturn += "\t"+CatalogManager.getAttriName(tmpTableName, i);
												   }
												   for(int i=0;i<CatalogManager.getTableAttriNum(tmpTableName2);i++){ //输出属性名
													   System.out.print("\t"+CatalogManager.getAttriName(tmpTableName2, i));
													   selectReturn += "\t"+CatalogManager.getAttriName(tmpTableName2, i);
												   }
												   System.out.println();
												   selectReturn += "\n";
												   Vector<tuple> seleteTuples=API.join(tmpName1[0],tmpName1[1],tmpName2[0],tmpName2[1]);
												   for(int i=0;i<seleteTuples.size();i++){
													   System.out.println(seleteTuples.get(i).getString());
													   selectReturn += seleteTuples.get(i).getString() + "\t";
													   selectReturn += "\n";
												   }
												   return selectReturn;
											   }
											   else if(!isSynCorrect) continue;
											   else{
												   System.out.println(semaErrMsg+", select tuples failed");
												   isSemaCorrect=true;
												   return semaErrMsg+", select tuples failed";
											   }
										   }
										   else{
											   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
											   continue;
										   }

									   }
									   else{
										   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
										   continue;
									   }
								   }
								   else{
									   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
									   continue;
								   }
							   }
							   else{
								   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
								   continue;
							   }

//									 continue;
						   }
						   conditionNode tmpConditionNode=ParsingCondition(lexer,tmpTableName,";");
						   if(thetoken.toString().equals(";")){//select from 表名；
							   if(isSemaCorrect&&isSynCorrect){
								   /*
									* 执行select where 操作*/

								   String result = showSelectRes(tmpTableName,tmpAttriNames, tmpConditionNode,null,false);
								   System.out.println(result);
								   return result;
							   }
							   else if(!isSynCorrect) continue;
							   else{
								   System.out.println(semaErrMsg+", select tuples failed");
								   isSemaCorrect=true;
								   return semaErrMsg+", select tuples failed";
							   }

						   }
						   else if(isSynCorrect&&thetoken.tag==Tag.ORDER){
							   thetoken=lexer.scan();
							   if(thetoken.tag==Tag.BY){
								   thetoken=lexer.scan();
								   if(thetoken.tag==Tag.ID){
									   String tmpOrderAttriName=thetoken.toString();
									   if(isSemaCorrect&&!CatalogManager.isAttributeExist(tmpTableName, tmpOrderAttriName)){
										   semaErrMsg="The attribute "+tmpOrderAttriName+" doesn't exist";
										   isSemaCorrect=false;
									   }
									   thetoken=lexer.scan();
									   if(thetoken.toString().equals(";")||thetoken.tag==Tag.ASC||thetoken.tag==Tag.DESC){
										   boolean order;
										   if(thetoken.toString().equals(";")) order=true;
										   else {
											   order=thetoken.tag==Tag.ASC?true:false;
											   thetoken=lexer.scan();
											   if(isSynCorrect&&!thetoken.toString().equals(";")){
												   synErrMsg="Synthetic error near: "+thetoken.toString();
												   isSynCorrect=false;
												   continue;
											   }
										   }
										   if(isSemaCorrect){
											   /*执行select where order操作*/
											   String result = showSelectRes(tmpTableName,tmpAttriNames, tmpConditionNode,tmpOrderAttriName,order);
											   System.out.println(result);
											   return result;
										   }
										   else{
											   System.out.println(semaErrMsg+", select tuples failed");
											   isSemaCorrect=true;
											   return semaErrMsg+", select tuples failed";
										   }
									   }
									   else{
										   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
										   continue;
									   }

								   } else{
									   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
									   continue;
								   }
							   }
							   else{
								   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
								   continue;
							   }
						   }
						   else{
							   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
							   continue;
						   }
					   }
					   else if(thetoken.toString().equals(";")){//select * from 表名；
						   if(isSemaCorrect){
							   /*执行select 操作*/
							   String result = showSelectRes(tmpTableName,tmpAttriNames, null,null,false);
							   System.out.println(result);
							   return result;
						   }
						   else{
							   System.out.println(semaErrMsg+", select tuples failed");
							   isSemaCorrect=true;
						   }
					   }
					   else if(thetoken.tag==Tag.ORDER){
						   thetoken=lexer.scan();
						   if(thetoken.tag==Tag.BY){
							   thetoken=lexer.scan();
							   if(thetoken.tag==Tag.ID){
								   String tmpOrderAttriName=thetoken.toString();
								   if(isSemaCorrect&&!CatalogManager.isAttributeExist(tmpTableName, tmpOrderAttriName)){
									   semaErrMsg="The attribute "+tmpOrderAttriName+" doesn't exist";
									   isSemaCorrect=false;
								   }
								   thetoken=lexer.scan();
								   if(thetoken.toString().equals(";")||thetoken.tag==Tag.ASC||thetoken.tag==Tag.DESC){
									   boolean order;
									   if(thetoken.toString().equals(";")) order=true;
									   else {
										   order=thetoken.tag==Tag.ASC?true:false;
										   thetoken=lexer.scan();
										   if(isSynCorrect&&!thetoken.toString().equals(";")){
											   synErrMsg="Synthetic error near: "+thetoken.toString();
											   isSynCorrect=false;
											   continue;
										   }
									   }
									   if(isSemaCorrect){
										   /*
											* 执行select order操作
											*/
										   String result = showSelectRes(tmpTableName,tmpAttriNames, null,tmpOrderAttriName,order);
										   System.out.println(result);
										   return result;
									   }
									   else{
										   System.out.println(semaErrMsg+", select tuples failed");
										   isSemaCorrect=true;
										   return semaErrMsg+", select tuples failed";
									   }
								   }
								   else{
									   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
									   continue;
								   }

							   } else{
								   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
								   continue;
							   }
						   }
						   else{
							   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
							   continue;
						   }
					   }
					   else{

						   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
						   continue;
					   }
				   }
				   else{
					   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
					   continue;
				   }
			   }
			   else{
				   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
				   continue;
			   }


		   }
		   else if(thetoken.tag==Tag.SHOW){
			   System.out.println("show!");
			   thetoken=lexer.scan();
			   if(thetoken.toString().equals("tables")){
				   thetoken=lexer.scan();
				   if(thetoken.toString().equals(";")){
					   API.showTableCatalog();
				   } else{
					   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
					   continue;
				   }

			   }
			   else if(thetoken.toString().equals("indexes")){
				   thetoken=lexer.scan();
				   if(thetoken.toString().equals(";")){
					   API.showIndexCatalog();
				   } else{
					   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
					   continue;
				   }
			   }
			   else if(thetoken.toString().equals("catalog")){
				   thetoken=lexer.scan();
				   if(thetoken.toString().equals(";")){
					   API.showCatalog();
				   } else{
					   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
					   continue;
				   }
			   }
			   else{
				   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
				   continue;
			   }
		   }
		   else{
			   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
			   continue;
		   }

	   } //end of while
	   return "非法操作";
   }
 //显示 选择返回结果
 private static String showSelectRes(String tmpTableName,Vector<String> tmpAttriNames,conditionNode tmpConditionNode,String tmpOrderAttriName,boolean order){
	 System.out.println("In function showSelectRes");
     String result = "";
	 if(tmpAttriNames==null)
		 for(int i=0;i<CatalogManager.getTableAttriNum(tmpTableName);i++){ //输出属性名
			 System.out.print("\t"+CatalogManager.getAttriName(tmpTableName, i));
			 result += "\t"+CatalogManager.getAttriName(tmpTableName, i);
		 }
	 else
		 for(int i=0;i<tmpAttriNames.size();i++) {
			 System.out.print("\t"+tmpAttriNames.get(i));
			 result += "\t"+tmpAttriNames.get(i);
		 }
	 System.out.println();
	 result += "\n";
	 Vector<tuple> seleteTuples;
	 if(tmpOrderAttriName==null)
		 seleteTuples=API.selectTuples(tmpTableName,tmpAttriNames, tmpConditionNode);
	 else{
		 seleteTuples=API.selectTuples(tmpTableName,tmpAttriNames, tmpConditionNode,tmpOrderAttriName,order);
	 }
	 for(int i=0;i<seleteTuples.size();i++){
		 System.out.println(seleteTuples.get(i).getString());
		 result += seleteTuples.get(i).getString() + "\t";
		 result += "\n";
	 }
	 System.out.println("There are "+seleteTuples.size()+" tuples returned \n");
	 result += "There are "+seleteTuples.size()+" tuples returned \n";
	 return result;
 }
//对project部分语句进行解析
private static Vector<String> ParsingProjection(Lexer lexer) throws IOException{
	   Vector<String>tmpAttriNames=new Vector<String>();
	   thetoken=lexer.scan();
	   if(thetoken.toString().equals("*")){
		   thetoken=lexer.scan();
		   return null;
	   }
	   else{
		   while(thetoken.tag!=Tag.FROM){
			   if(thetoken.tag==Tag.ID){
				   tmpAttriNames.add(thetoken.toString());
				   thetoken=lexer.scan();
				   if(thetoken.toString().equals(",")){
					   thetoken=lexer.scan();
				   }
				   else if(thetoken.tag==Tag.FROM);
				   else{
					   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
						 break;
				   }
			   }
			   else{
				   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
					 break;
			   }
			   
		   }
		   return tmpAttriNames;
	   }
   }
//对条件的每个表达式进行解析
private static conditionNode ParsingExpression(Lexer lexer,String tmpTableName) throws IOException{
	   String tmpAttriName;Comparison op;String tmpValue;	
	   	boolean constantFlag = false;
	   if(thetoken.tag==Tag.ID){
		   tmpAttriName=thetoken.toString();
		   if(isSemaCorrect&&!CatalogManager.isAttributeExist(tmpTableName, tmpAttriName)){
			   isSemaCorrect=false;
				 semaErrMsg="The attribute "+tmpAttriName+" doesn't exist";
		   }
		   thetoken=lexer.scan();
		   if(thetoken.tag==Tag.OP){
			   op=Comparison.parseCompar(thetoken);
			   thetoken=lexer.scan();
			   tmpValue=thetoken.toString();
			   if(isSemaCorrect){
				   if(thetoken.tag==Tag.STR){
					   constantFlag=true;
					   String tmpType=CatalogManager.getType(tmpTableName, tmpAttriName);
					   int tmpLength=CatalogManager.getLength(tmpTableName, tmpAttriName);
					 
					   if(!tmpType.equals("char")
								 ||tmpLength<tmpValue.getBytes().length){
							 isSemaCorrect=false;
							 semaErrMsg="The type of value +"+tmpValue+" should be "+tmpType+"("+tmpLength+"), not char("+tmpValue.getBytes().length+")";
					   }
				   }
				   else if(thetoken.tag==Tag.INTNUM){
					   constantFlag=true;
					   String tmpType=CatalogManager.getType(tmpTableName, tmpAttriName);
					   int tmpLength=CatalogManager.getLength(tmpTableName, tmpAttriName);
					   
						 if(!tmpType.toString().equals("int")
								 &&!tmpType.equals("float")){
							isSemaCorrect=false;
							 semaErrMsg="The type of value +"+tmpValue+" should be "+tmpType+"("+tmpLength+"), not int";
						 }
				   }
				   else if(thetoken.tag==Tag.FLOATNUM){
					   constantFlag=true;
					   String tmpType=CatalogManager.getType(tmpTableName, tmpAttriName);
					   int tmpLength=CatalogManager.getLength(tmpTableName, tmpAttriName);
					   
					   if(!tmpType.equals("float")){
							isSemaCorrect=false;
							 semaErrMsg="The type of value +"+tmpValue+" should be "+tmpType+"("+tmpLength+"), not float"; 
						}
				   }
				   else if(thetoken.tag==Tag.ID){//属性间比较	
					   constantFlag=false;
					   String tmpType1=CatalogManager.getType(tmpTableName, tmpAttriName);
					   String tmpType2=CatalogManager.getType(tmpTableName, tmpValue);
					   //支持float或int或char的属性间比较
					   if(!tmpType1.equals(tmpType2)){
						   isSemaCorrect=false;
							 semaErrMsg="The two attributes are in different types and cannot be compared"; 
					   }
				   }
				   else{
						if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();
						isSynCorrect=false;
						
				   }				   
				   //return new conditionNode(tmpAttriName,op,tmpValue,constantFlag);
				    
			   }
			   return new conditionNode(tmpAttriName,op,tmpValue,constantFlag);
			   
		   }
		   else{
				if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();
				isSynCorrect=false;
		   }
	   }
	   else{
		   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();
			isSynCorrect=false; 
	   }
	return null;	   
   }
//对条件部分字符串进行解析
private static conditionNode ParsingCondition(Lexer lexer,String tmpTableName,String endtoken)throws IOException {
	   /*
	    * 语义错误种类
	    * 1 属性名不存在
	    * 2 value 格式不对
	    * 3 操作符不对 char只支持= <>
	    */
	   conditionNode tmpConditionRoot = null;
	   conditionNode tmpExpresstion = null,tmpConjunction;
	   thetoken=lexer.scan();
	   boolean flag=false;//如果第一个式子是带括号的 flag==true 以保证其完整性
	   if(thetoken.toString().equals("(")){
		   tmpConditionRoot=ParsingCondition(lexer,tmpTableName,")");
		   flag=true;
	   }
	   else if(thetoken.tag==Tag.ID){
		   tmpConditionRoot=ParsingExpression(lexer,tmpTableName);
	   }
	   else{
		   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();
			isSynCorrect=false; 
	   }
	   if(tmpConditionRoot==null||!isSynCorrect){
		   return null;
	   }
	 
	   thetoken=lexer.scan();	
	   while(!thetoken.toString().equals(endtoken)&&thetoken.tag!=Tag.ORDER){
		   if(thetoken.tag==Tag.AND){
			   tmpConjunction=new conditionNode("and");
			   thetoken=lexer.scan();
			   if(thetoken.toString().equals("(")){
				   tmpExpresstion=ParsingCondition(lexer,tmpTableName,")");
			   }
			   else if(thetoken.tag==Tag.ID){
				   tmpExpresstion=ParsingExpression(lexer,tmpTableName);
			   }
			   else{
				   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();
					isSynCorrect=false; 
			   }
			   if(tmpExpresstion==null){
				   return null;
			   }
			   //建树
			   if(tmpConditionRoot.conjunction=="or"&&flag==false){
				   
				   tmpConditionRoot=tmpConditionRoot.linkChildNode(tmpConditionRoot.left, tmpConjunction.linkChildNode(tmpConditionRoot.right, tmpExpresstion));   
				   
			   }
			   
			   else{
				   tmpConditionRoot=tmpConjunction.linkChildNode(tmpConditionRoot, tmpExpresstion);
				   if(flag) flag=false;
			   }
				   
			  
				   
		   }
		   else if(thetoken.tag==Tag.OR){
			   tmpConjunction=new conditionNode("or");
			   thetoken=lexer.scan();
			   if(thetoken.toString().equals("(")){
				   tmpExpresstion=ParsingCondition(lexer,tmpTableName,")");
			   }
			   else if(thetoken.tag==Tag.ID){
				   tmpExpresstion=ParsingExpression(lexer,tmpTableName);
			   }
			   else{
				   if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();
					isSynCorrect=false; 
			   }
			  
			   if(tmpExpresstion==null){
				   return null;
			   }
			 //建树
			   tmpConditionRoot=tmpConjunction.linkChildNode(tmpConditionRoot, tmpExpresstion);
		   }
		   
		   else if(thetoken.toString().equals(endtoken)||thetoken.tag==Tag.ORDER);
		   else{
				if(isSynCorrect)  synErrMsg="Synthetic error near: "+thetoken.toString();isSynCorrect=false;
				break;
		   }
			thetoken=lexer.scan(); 
			
	   }
	 
	return tmpConditionRoot;
	
   }

}
	   


