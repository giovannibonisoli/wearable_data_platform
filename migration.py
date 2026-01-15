"""
Migration script to add multi-user support to the Lively Ageing system.
Run this script once to migrate from single admin to multi-user system.
"""

import os
import bcrypt
from db import DatabaseManager

def migrate_to_multiuser():
    """Migrate database to support multiple admin users"""
    
    db = DatabaseManager()
    if not db.connect():
        print("❌ Failed to connect to database")
        return False
    
    try:
        print("🔄 Starting migration to multi-user support...")
        
        # Step 1: Create admin_users table
        print("\n1. Creating admin_users table...")
        db.execute_query("""
            CREATE TABLE IF NOT EXISTS admin_users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                full_name VARCHAR(255),
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMPTZ,
                is_active BOOLEAN DEFAULT TRUE
            );
        """)
        print("✅ admin_users table created")
        
        # Step 2: Add admin_user_id column to email_addresses
        print("\n2. Adding admin_user_id column to email_addresses...")
        db.execute_query("""
            ALTER TABLE email_addresses 
            ADD COLUMN IF NOT EXISTS admin_user_id INTEGER REFERENCES admin_users(id);
        """)
        print("✅ admin_user_id column added")
        
        # Step 3: Create the first admin user from environment variables
        print("\n3. Creating initial admin user from environment variables...")
        username = os.getenv('log_USERNAME')
        password = os.getenv('PASSWORD')
        
        if not username or not password:
            print("⚠️  Warning: USERNAME or PASSWORD not found in environment variables")
            print("    You'll need to create an admin user manually")
        else:
            # Check if user already exists
            existing = db.execute_query("""
                SELECT id FROM admin_users WHERE username = %s
            """, (username,))
            
            if existing:
                admin_user_id = existing[0][0]
                print(f"✅ Admin user '{username}' already exists (ID: {admin_user_id})")
            else:
                password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                result = db.execute_query("""
                    INSERT INTO admin_users (username, password_hash, full_name)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, (username, password_hash, 'System Administrator'))
                
                if result:
                    admin_user_id = result[0][0]
                    print(f"✅ Created admin user '{username}' (ID: {admin_user_id})")
                else:
                    print("❌ Failed to create admin user")
                    return False
            
            # Step 4: Assign existing email addresses to the admin user
            print("\n4. Assigning existing email addresses to admin user...")
            result = db.execute_query("""
                UPDATE email_addresses 
                SET admin_user_id = %s 
                WHERE admin_user_id IS NULL
                RETURNING id
            """, (admin_user_id,))
            
            if result:
                count = len(result)
                print(f"✅ Assigned {count} email address(es) to admin user")
            else:
                print("ℹ️  No unassigned email addresses found")
        
        # Step 5: Create index for performance
        print("\n5. Creating indexes...")
        db.execute_query("""
            CREATE INDEX IF NOT EXISTS idx_email_addresses_admin_user 
            ON email_addresses(admin_user_id);
        """)
        print("✅ Indexes created")
        
        print("\n✅ Migration completed successfully!")
        print("\n📋 Next steps:")
        print("   1. Update your app.py with the new authentication routes")
        print("   2. Update your DatabaseManager class with new methods")
        print("   3. Test the login with your existing credentials")
        print("   4. Consider creating additional admin users via /register route")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        db.rollback()
        return False
    finally:
        db.close()

def create_additional_admin(username, password, full_name=None):
    """Helper function to create additional admin users"""
    db = DatabaseManager()
    if not db.connect():
        print("❌ Failed to connect to database")
        return False
    
    try:
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        result = db.execute_query("""
            INSERT INTO admin_users (username, password_hash, full_name)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (username, password_hash, full_name))
        
        if result:
            user_id = result[0][0]
            print(f"✅ Created admin user '{username}' (ID: {user_id})")
            return user_id
        else:
            print("❌ Failed to create admin user")
            return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None
    finally:
        db.close()

if __name__ == "__main__":
    print("=" * 60)
    print("Lively Ageing - Multi-User Migration Script")
    print("=" * 60)
    
    # Run migration
    # success = migrate_to_multiuser()
    
    
    print("\n" + "=" * 60)
    print("Would you like to create additional admin users? (y/n)")
    response = input().strip().lower()
        
    while response == 'y':
        print("\nEnter new admin user details:")
        username = input("Username: ").strip()
        password = input("Password: ").strip()
        full_name = input("Full Name (optional): ").strip() or None
            
        create_additional_admin(username, password, full_name)
            
        print("\nCreate another admin user? (y/n)")
        response = input().strip().lower()
    
    print("\n" + "=" * 60)
    print("Migration script completed")
    print("=" * 60)