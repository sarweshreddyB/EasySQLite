//
//  EasySQLite.swift
//  EasySQLite
//
//  Created by sarwesh reddy on 12/11/19.
//  Copyright © 2019 sarwesh reddy. All rights reserved.
//

import Foundation
import SQLite3
class EasySQLite
{

    internal let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
    internal let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
    var statement: OpaquePointer?
    let busyTimeOut:Int32 = 60 * 1000
    
    /*opens a database connect for a given dbName if db exists else creates a new db*/
    func openDB(dbName: String) -> OpaquePointer? {
        let fileUrl = try! FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
        .appendingPathComponent("\(dbName).db")
        var db:OpaquePointer?
        if sqlite3_open(fileUrl.path, &db) != SQLITE_OK {
          print("error opening database")
        }
        return db
    }
    
    /* closes a db connection*/
    func closeDB(db:OpaquePointer?) {
        if sqlite3_close(db) != SQLITE_OK {
          print("error closing database")
        }
    }
    /* begins a transaction*/
    func beginTransaction(db:OpaquePointer?) {
        if sqlite3_exec(db, "BEGIN TRANSACTION", nil, nil, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("error begining transaction for database with Error: \(errmsg)")
            
        }
        statement = nil

        //BEGIN TRANSACTION
    }
    /*roll back transaction*/
    func rollBackTransaction(db:OpaquePointer?) {
        if sqlite3_exec(db, "ROLLBACK", nil, nil, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("error rollbacking transaction for database with Error: \(errmsg)")
            
        }
        statement = nil
        
    }
    /*commit transaction*/
    func commitTransaction(db:OpaquePointer?) {
        if sqlite3_exec(db, "COMMIT", nil, nil, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("error commiting transaction for database with Error: \(errmsg)")
            
        }
        statement = nil
        
    }

    /*Creates a table with the given query and you can also execute DDL Queries like Alter commands*/
    func createTable(query: String,db:OpaquePointer?) {
        if sqlite3_exec(db, query, nil, nil, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("error creating table for \(query): \(errmsg)")
            
        }
        statement = nil
    }
    
    func insertvalues(table: String,values: [String: Any?] ,db:OpaquePointer?)     {
        sqlite3_busy_timeout(db, busyTimeOut)

        var statement: OpaquePointer?
        var sql = "insert into \(table)"
        var valueString = ""
        
        if values.count > 0 {
            sql += "("
            var seperatorNeeded = false
            
            for (key,_) in values {
                
                if seperatorNeeded {
                    sql += ","
                    valueString += ","
                }
                
                seperatorNeeded = true
                sql += key
                valueString += ":\(key)"
            }
            sql += ")"
            
        } else {
            
            sql += "()"
            valueString += "NULL"
            
        }
        sql += "values(\(valueString))"
        
        if sqlite3_prepare_v2(db, sql, -1, &statement, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("error preparing insert: \(errmsg)")
        }
        
        statement = bind(values,statement: statement)
        
        if sqlite3_step(statement) != SQLITE_DONE {
            if sqlite3_step(statement) == SQLITE_BUSY {
                sqlite3_busy_timeout(db, busyTimeOut)
            } else {
                let errmsg = String(cString: sqlite3_errmsg(db)!)
              print("failure inserting foo: \(errmsg)")
            }
        }
        
        if sqlite3_finalize(statement) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("error finalizing prepared statement: \(errmsg)")
        }
        
        statement = nil
    }
    
    func updateValues(table: String,values: [String: Any?] ,db:OpaquePointer?, whereClause: String?,whereArgs: [String?]?) -> Bool {
        sqlite3_busy_timeout(db, busyTimeOut)
        var success = true
        var statement: OpaquePointer?
        var sql = "update \(table) set "
        // var keyString = ""
        var valueString = ""
        if values.count > 0 {
            // sql += "("
            var seperatorNeeded = false
            
            for (key,_) in values {
                
                if seperatorNeeded {
                    sql += ","
                    valueString += ","
                }
                seperatorNeeded = true
                sql += key
                sql += " = :\(key)"
            }
            sql += appendClause(name: " where", clause: whereClause)
            // sql += ")"
        } else {
            //sql += "()"
            valueString += "NULL"
        }
        // sql += "values(\(valueString))"
        //print(sql)
        
        if sqlite3_prepare_v2(db, sql, -1, &statement, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("error preparing update \(table): \(errmsg)")
            success = false
        }
        
        statement = bind(values,statement: statement)
        
        var idx = values.count + 1
        
        if whereArgs != nil {
            
            for arg in whereArgs! {
                
                if arg != nil {
                    bind(arg, atIndex: idx, statement: statement)
                    idx += 1
                }
            }
        }
        
        if sqlite3_step(statement) != SQLITE_DONE {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("failure updating \(table): \(errmsg)")
            rollBackTransaction(db:db)
            success = false
        }
        
        if sqlite3_finalize(statement) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("error finalizing prepared statement: \(errmsg)")
            rollBackTransaction(db:db)
            success = false
        }
        
        statement = nil
      return success
    }
    
    
    func query(db: OpaquePointer?,tables: String,distinct: Bool?,columns: [String?]?,selection: String?,selectionArgs: [String?]?,groupBy: String?,having:String?, orderBy: String?,limit: String?) -> [[String: Any?]] {
        sqlite3_busy_timeout(db, busyTimeOut)

        var statement: OpaquePointer?
        
        var sql = "select "
        
        if distinct != nil && distinct!  {
            sql += "distinct "
        }
        
        if columns != nil {
            
            sql += appendColumns(columns: columns!)
        } else {
            
            sql += "*"
            
        }
        sql += " from \(tables) \(appendClause(name: "where", clause: selection))"
        sql += "\(appendClause(name: "group by", clause: groupBy))"
        sql += "\(appendClause(name: "having", clause: having))"
        sql += "\(appendClause(name: "order by", clause: orderBy))"
        sql += "\(appendClause(name: "limit", clause: limit))"
        //     print(sql)
        
        if sqlite3_prepare_v2(db, sql, -1, &statement, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("error preparing select: \(errmsg)")
            rollBackTransaction(db:db)
        }
        
        var idx = 1
        if selectionArgs != nil {
            
            for arg in selectionArgs! {
                
                if arg != nil {
                    bind(arg, atIndex: idx, statement: statement)
                    idx += 1
                }
            }
        }
        
        idx = 1
        
        var contentValues = [[String: Any?]]()
        
        while sqlite3_step(statement) == SQLITE_ROW {
            var contentValue = [String: Any?]()
            var cnt = sqlite3_column_count(statement)
            //          print(cnt)
            
            while cnt > 0 {
                
                cnt -= 1
                
                if sqlite3_column_type(statement, cnt) == SQLITE_INTEGER {
                    let value = sqlite3_column_int64(statement, cnt)
                    contentValue["\(String(cString: sqlite3_column_name(statement, Int32(cnt))!))"]  = value
                }
                
                if sqlite3_column_type(statement, cnt) == SQLITE_FLOAT {
                    let value = sqlite3_column_double(statement, cnt)
                    contentValue["\(String(cString: sqlite3_column_name(statement, Int32(cnt))!))"]  = value
                }
                
                if sqlite3_column_type(statement, cnt) == SQLITE_TEXT {
                    let cString1 = sqlite3_column_text(statement, cnt)
                    let value = String(cString: cString1!)
                    contentValue["\(String(cString: sqlite3_column_name(statement, Int32(cnt))!))"]  = value
                }
            }
            
            contentValues.append(contentValue)
            idx += 1
        }
        
        if sqlite3_finalize(statement) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("error finalizing prepared statement: \(errmsg)")
            rollBackTransaction(db:db)
        }
        
        statement = nil
        return contentValues
    }
    
    func appendClause(name: String,clause: String?) -> String {
        var clauseString = ""
        if clause != nil {
            clauseString += "\(name) \(clause!) "
            
        }
        return clauseString
    }
    
    func appendColumns(columns: [String?]) -> String {
        var columnsString = ""
        var i = 0
        for column in columns {
            
            if column != nil {
                
                if i > 0 {
                    columnsString += ","
                }
                columnsString += "\(column!)"
            }
            i += 1
        }
        
        return columnsString
    }
    
    func delete(table: String,db:OpaquePointer?,whereClause: String?,whereArgs:[String?]?)
    {
        sqlite3_busy_timeout(db, busyTimeOut)

        var statement: OpaquePointer?
        var sql = "delete from \(table) "
        sql += appendClause(name: "where", clause: whereClause)
        
        if sqlite3_prepare_v2(db, sql, -1, &statement, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("error preparing select: \(errmsg)")
            rollBackTransaction(db:db)
        }
        
        var idx = 1
        if whereArgs != nil {
            
            for arg in whereArgs! {
                
                if arg != nil {
                    bind(arg, atIndex: idx, statement: statement)
                    idx += 1
                }
            }
        }
        
        if sqlite3_step(statement) != SQLITE_DONE {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("failure inserting foo: \(errmsg)")
            rollBackTransaction(db:db)
        }
        
        if sqlite3_finalize(statement) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("error finalizing prepared statement: \(errmsg)")
            rollBackTransaction(db:db)
        }
        
        statement = nil
    }
    
    func rawQuery(sql: String,selectionArgs: [String?]?,db: OpaquePointer?) -> [[String:Any?]] {
        sqlite3_busy_timeout(db, busyTimeOut)
  
        var statement: OpaquePointer?
        if sqlite3_prepare_v2(db, sql, -1, &statement, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("error preparing select: \(errmsg)")
            rollBackTransaction(db:db)
        }
        
        var idx = 1
        if selectionArgs != nil {
            
            for arg in selectionArgs! {
                
                if arg != nil {
                    bind(arg, atIndex: idx, statement: statement)
                    idx += 1
                }
            }
        }
        
        idx = 1
        var contentValues = [[String: Any?]]()
        while sqlite3_step(statement) == SQLITE_ROW {
            var contentValue = [String: Any?]()
            var cnt = sqlite3_column_count(statement)
            //          print(cnt)
            
            while cnt > 0 {
                
                cnt -= 1
                
                if sqlite3_column_type(statement, cnt) == SQLITE_INTEGER {
                    let value = sqlite3_column_int64(statement, cnt)
                    contentValue["\(String(cString: sqlite3_column_name(statement, Int32(cnt))!))"]  = value
                }
                
                if sqlite3_column_type(statement, cnt) == SQLITE_FLOAT {
                    let value = sqlite3_column_double(statement, cnt)
                    contentValue["\(String(cString: sqlite3_column_name(statement, Int32(cnt))!))"]  = value
                }
                
                if sqlite3_column_type(statement, cnt) == SQLITE_TEXT {
                    let cString1 = sqlite3_column_text(statement, cnt)
                    let value = String(cString: cString1!)
                    contentValue["\(String(cString: sqlite3_column_name(statement, Int32(cnt))!))"]  = value
                }
                
            }
            
            contentValues.append(contentValue)
            idx += 1
        }
        
        if sqlite3_finalize(statement) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
          print("error finalizing prepared statement: \(errmsg)")
            rollBackTransaction(db:db)
        }
        
        statement = nil
        return contentValues
    }
    
    func rawQueryCursor(sql: String,selectionArgs: [String?]?,db: OpaquePointer?) -> OpaquePointer? {
        sqlite3_busy_timeout(db, busyTimeOut)
        
        var statement: OpaquePointer?
        if sqlite3_prepare_v2(db, sql, -1, &statement, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            print("error preparing select: \(errmsg)")
            rollBackTransaction(db:db)
        }
        
        var idx = 1
        if selectionArgs != nil {
            
            for arg in selectionArgs! {
                
                if arg != nil {
                    bind(arg, atIndex: idx, statement: statement)
                    idx += 1
                }
            }
        }
        
        idx = 1
        return statement
    }
    func getContentValues(statement:OpaquePointer?,db:OpaquePointer?) -> [[String: Any?]] {
        var contentValues = [[String: Any?]]()
        
        while sqlite3_step(statement) == SQLITE_ROW {
            var contentValue = [String: Any?]()
            var cnt = sqlite3_column_count(statement)
            //          print(cnt)
            
            while cnt > 0 {
                
                cnt -= 1
                
                if sqlite3_column_type(statement, cnt) == SQLITE_INTEGER {
                    let value = sqlite3_column_int64(statement, cnt)
                    contentValue["\(String(cString: sqlite3_column_name(statement, Int32(cnt))!))"]  = value
                }
                
                if sqlite3_column_type(statement, cnt) == SQLITE_FLOAT {
                    let value = sqlite3_column_double(statement, cnt)
                    contentValue["\(String(cString: sqlite3_column_name(statement, Int32(cnt))!))"]  = value
                }
                
                if sqlite3_column_type(statement, cnt) == SQLITE_TEXT {
                    let cString1 = sqlite3_column_text(statement, cnt)
                    let value = String(cString: cString1!)
                    contentValue["\(String(cString: sqlite3_column_name(statement, Int32(cnt))!))"]  = value
                }
                
            }
            
            contentValues.append(contentValue)
            //idx += 1
        }
        
        if sqlite3_finalize(statement) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            print("error finalizing prepared statement: \(errmsg)")
            rollBackTransaction(db:db)
        }
        
       // statement = nil
        return contentValues
    }
    func attachDatabase(db:OpaquePointer?) {//combining 2 data bases
        var statement: OpaquePointer?
        
        let fileURL1 = try! FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
            .appendingPathComponent("test.sqlite")
        //      print(fileURL1.path)
        
        if sqlite3_exec(db,"attach '\(fileURL1.path)' as tempar", nil,nil,nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            rollBackTransaction(db:db)

          print("error creating table: \(errmsg)")
        }
        
        if sqlite3_prepare_v2(db, "SELECT a.id,a.name,age,mobile FROM customerDetails AS a,tempar.test AS b WHERE a.id = b.id", -1, &statement, nil) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            rollBackTransaction(db:db)

          print("error preparing select: \(errmsg)")
        }
        
        while sqlite3_step(statement) == SQLITE_ROW {
            
            let id = sqlite3_column_int64(statement, 0)
            
          print("id = \(id); ", terminator: "")
            
            let cString = sqlite3_column_text(statement, 1)
            let name = String(cString: cString!)
          print("name = \(name)")
            
            let age = sqlite3_column_int64(statement, 2)
          print("age = \(age); ", terminator: "")
            
            let cString1 = sqlite3_column_text(statement, 3)
            let mobile = String(cString: cString1!)
          print("mobile = \(mobile)")
            
        }
        
        if sqlite3_finalize(statement) != SQLITE_OK {
            let errmsg = String(cString: sqlite3_errmsg(db)!)
            rollBackTransaction(db:db)
          print("error finalizing prepared statement: \(errmsg)")
        }
        
        statement = nil
        
    }
    
    public func bind(_ values: [String: Any?], statement: OpaquePointer?) -> OpaquePointer? {
        for (name, value) in values {
            
            let idx = sqlite3_bind_parameter_index(statement, ":\(name)")
            guard idx > 0 else {
                fatalError("parameter not found: \(name)")
            }
            bind(value, atIndex: Int(idx), statement: statement)
        }
         
        return statement
    }
    
    fileprivate func bind(_ value: Any?, atIndex idx: Int, statement: OpaquePointer?) {
        
        if value == nil {
            sqlite3_bind_null(statement, Int32(idx))
        } else if let value = value as? Double {
            sqlite3_bind_double(statement, Int32(idx), value)
        } else if let value = value as? Int64 {
            sqlite3_bind_int64(statement, Int32(idx), value)
        } else if let value = value as? String {
            sqlite3_bind_text(statement, Int32(idx), value, -1, SQLITE_TRANSIENT)
        } else if let value = value as? Int {
            sqlite3_bind_int64(statement, Int32(idx), Int64(value))
            // self.bind(value, atIndex: idx)
        } else if let value = value as? Int8 {
            sqlite3_bind_int64(statement, Int32(idx), Int64(value))
            // self.bind(value, atIndex: idx)
        }else if let _ = value as? Bool {
            //self.bind(value, atIndex: idx)
        }else if let value = value as? Date {
            sqlite3_bind_text(statement, Int32(idx), String(describing: value), -1, SQLITE_TRANSIENT)
            //self.bind(value, atIndex: idx)
        }else if let value = value as? Float32 {
            sqlite3_bind_double(statement, Int32(idx), Double(value))
    } else if let value = value  {
            fatalError("tried to bind unexpected value \(value)")
        }
    }
    


    func updateSequence(table:String,db: OpaquePointer?) {
        sqlite3_busy_timeout(db, busyTimeOut)

        let customerSequence = query(db:db, tables: "SQLITE_SEQUENCE", distinct: nil, columns: ["seq"], selection: "name = ?", selectionArgs: ["\(table)"], groupBy: nil, having: nil, orderBy: nil, limit: nil)
        let now = Int64(NSDate().timeIntervalSince1970 * 1000)
        
        if customerSequence.count > 0 {
            let seq = customerSequence[0]["seq"]! as! Int64
            if seq != 0 {
                
               _ = updateValues(table: "SQLITE_SEQUENCE", values: ["seq":now], db: db, whereClause: "name = ?", whereArgs: ["\(table)"])
            }  else {
            insertvalues(table: "SQLITE_SEQUENCE", values: ["name":"\(table)","seq":now], db: db)
            }
        } else {
            insertvalues(table: "SQLITE_SEQUENCE", values: ["name":"\(table)","seq":now], db: db)
        }
    }


    
    
}
