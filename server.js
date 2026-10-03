// server.ts
import path2 from "path";
import fs2 from "fs";
import express2 from "express";
import { createServer as createViteServer } from "vite";

// server/db.ts
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { MongoClient } from "mongodb";
var JsonFileCollection = class {
  constructor(filePath) {
    this.items = [];
    this.indexes = /* @__PURE__ */ new Set();
    this.filePath = filePath;
    this.load();
  }
  load() {
    try {
      const dir = path.dirname(this.filePath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
      if (fs.existsSync(this.filePath)) {
        const data = fs.readFileSync(this.filePath, "utf-8");
        this.items = JSON.parse(data);
      } else {
        this.items = [];
        this.save();
      }
    } catch (err) {
      console.error(`Error reading ${this.filePath}:`, err);
      this.items = [];
    }
  }
  save() {
    try {
      const dir = path.dirname(this.filePath);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
      fs.writeFileSync(this.filePath, JSON.stringify(this.items, null, 2), "utf-8");
    } catch (err) {
      console.error(`Error saving ${this.filePath}:`, err);
    }
  }
  matchQuery(item, query) {
    if (!query) return true;
    if (typeof query === "function") {
      return query(item);
    }
    return Object.entries(query).every(([key, val]) => {
      return item[key] === val;
    });
  }
  async findOne(query) {
    const item = this.items.find((i) => this.matchQuery(i, query));
    return item ? JSON.parse(JSON.stringify(item)) : null;
  }
  async find(query) {
    const matched = this.items.filter((i) => this.matchQuery(i, query));
    return JSON.parse(JSON.stringify(matched));
  }
  async insertOne(doc) {
    const newDoc = {
      ...doc,
      _id: doc._id || crypto.randomUUID()
    };
    this.items.push(newDoc);
    this.save();
    return JSON.parse(JSON.stringify(newDoc));
  }
  async insertMany(docs) {
    const created = [];
    for (const doc of docs) {
      const newDoc = {
        ...doc,
        _id: doc._id || crypto.randomUUID()
      };
      this.items.push(newDoc);
      created.push(newDoc);
    }
    this.save();
    return JSON.parse(JSON.stringify(created));
  }
  async updateOne(query, update) {
    const index = this.items.findIndex((i) => this.matchQuery(i, query));
    if (index === -1) {
      return { modifiedCount: 0, matchedCount: 0 };
    }
    const current = this.items[index];
    const updateObj = update;
    if (updateObj.$set) {
      Object.assign(current, updateObj.$set);
    }
    if (updateObj.$push) {
      for (const [key, pushVal] of Object.entries(updateObj.$push)) {
        if (!Array.isArray(current[key])) {
          current[key] = [];
        }
        current[key].push(pushVal);
      }
    }
    if (!updateObj.$set && !updateObj.$push) {
      Object.assign(current, update);
    }
    this.items[index] = current;
    this.save();
    return { modifiedCount: 1, matchedCount: 1 };
  }
  async deleteOne(query) {
    const index = this.items.findIndex((i) => this.matchQuery(i, query));
    if (index === -1) {
      return { deletedCount: 0 };
    }
    this.items.splice(index, 1);
    this.save();
    return { deletedCount: 1 };
  }
  async countDocuments(query) {
    return this.items.filter((i) => this.matchQuery(i, query)).length;
  }
  async createIndex(field) {
    this.indexes.add(field);
  }
};
var MongoCollectionWrapper = class {
  constructor(col) {
    this.col = col;
  }
  async findOne(query) {
    if (typeof query === "function") {
      const all = await this.col.find({}).toArray();
      const match = all.find(query);
      return match || null;
    }
    return this.col.findOne(query);
  }
  async find(query) {
    if (typeof query === "function") {
      const all = await this.col.find({}).toArray();
      return all.filter(query);
    }
    return this.col.find(query || {}).toArray();
  }
  async insertOne(doc) {
    const newDoc = {
      ...doc,
      _id: doc._id || crypto.randomUUID()
    };
    await this.col.insertOne(newDoc);
    return newDoc;
  }
  async insertMany(docs) {
    const formatted = docs.map((doc) => ({
      ...doc,
      _id: doc._id || crypto.randomUUID()
    }));
    if (formatted.length > 0) {
      await this.col.insertMany(formatted);
    }
    return formatted;
  }
  async updateOne(query, update) {
    let mongoUpdate = update;
    if (!update.$set && !update.$push) {
      mongoUpdate = { $set: update };
    }
    const res = await this.col.updateOne(query, mongoUpdate);
    return {
      modifiedCount: res.modifiedCount,
      matchedCount: res.matchedCount
    };
  }
  async deleteOne(query) {
    const res = await this.col.deleteOne(query);
    return { deletedCount: res.deletedCount };
  }
  async countDocuments(query) {
    if (typeof query === "function") {
      const all = await this.col.find({}).toArray();
      return all.filter(query).length;
    }
    return this.col.countDocuments(query || {});
  }
  async createIndex(field) {
    try {
      await this.col.createIndex({ [field]: 1 });
    } catch (e) {
      console.warn(`Failed to create index for ${field}:`, e);
    }
  }
};
var DatabaseManager = class {
  constructor() {
    this.isMongoRemote = false;
    this.initialized = false;
  }
  async init() {
    if (this.initialized) return;
    const mongoUri = process.env.MONGODB_URI;
    const isLocalhostDefault = !mongoUri || mongoUri.includes("localhost") || mongoUri.includes("127.0.0.1");
    if (mongoUri && !isLocalhostDefault) {
      try {
        console.log("Connecting to remote MongoDB at", mongoUri.replace(/\/\/([^:]+):([^@]+)@/, "//$1:***@"));
        const client = new MongoClient(mongoUri, { serverSelectionTimeoutMS: 3e3 });
        await client.connect();
        const db2 = client.db();
        this.users = new MongoCollectionWrapper(db2.collection("users"));
        this.issues = new MongoCollectionWrapper(db2.collection("issues"));
        this.notifications = new MongoCollectionWrapper(db2.collection("notifications"));
        this.isMongoRemote = true;
        console.log("Connected to remote MongoDB successfully");
      } catch (err) {
        console.warn("MongoDB connection failed, falling back to persistent JSON storage:", err.message);
        this.initFileDb();
      }
    } else {
      console.log("Using persistent embedded database storage (data/db_*.json)");
      this.initFileDb();
    }
    await this.users.createIndex("email");
    await this.issues.createIndex("issueId");
    await this.issues.createIndex("studentId");
    await this.issues.createIndex("status");
    await this.issues.createIndex("category");
    await this.notifications.createIndex("userId");
    this.initialized = true;
  }
  initFileDb() {
    const dataDir = path.resolve(process.cwd(), "data");
    this.users = new JsonFileCollection(path.join(dataDir, "db_users.json"));
    this.issues = new JsonFileCollection(path.join(dataDir, "db_issues.json"));
    this.notifications = new JsonFileCollection(path.join(dataDir, "db_notifications.json"));
    this.isMongoRemote = false;
  }
};
var db = new DatabaseManager();

// server/seed.ts
import bcrypt from "bcryptjs";
async function seedDemoData() {
  const existingUsersCount = await db.users.countDocuments();
  if (existingUsersCount > 0) {
    return;
  }
  console.log("Seeding initial demo data for CampusFix...");
  const salt = await bcrypt.genSalt(10);
  const adminHashedPassword = await bcrypt.hash("AdminPassword123!", salt);
  const studentHashedPassword = await bcrypt.hash("StudentPass123!", salt);
  const adminUser = {
    _id: "user_admin_001",
    name: "Campus Facility Administrator",
    email: "admin@campusfix.edu",
    password: adminHashedPassword,
    department: "Estate & Campus Facilities Directorate",
    year: "Faculty/Admin",
    role: "admin",
    createdAt: new Date(Date.now() - 30 * 864e5).toISOString()
  };
  const studentAlex = {
    _id: "user_student_001",
    name: "Alex Chen",
    email: "alex.chen@campusfix.edu",
    password: studentHashedPassword,
    department: "Computer Science & Engineering",
    year: "3rd Year",
    role: "student",
    createdAt: new Date(Date.now() - 25 * 864e5).toISOString()
  };
  const studentPriya = {
    _id: "user_student_002",
    name: "Priya Patel",
    email: "priya.patel@campusfix.edu",
    password: studentHashedPassword,
    department: "Mechanical Engineering",
    year: "2nd Year",
    role: "student",
    createdAt: new Date(Date.now() - 20 * 864e5).toISOString()
  };
  await db.users.insertMany([adminUser, studentAlex, studentPriya]);
  const sampleIssues = [
    {
      _id: "issue_cf_1001",
      issueId: "CF-2026-1001",
      studentId: studentAlex._id,
      studentName: studentAlex.name,
      studentEmail: studentAlex.email,
      studentDepartment: studentAlex.department,
      title: "Water leaking continuously in hostel 4th floor bathroom",
      description: "The overhead pipe joint in the 4th floor west wing washroom has cracked. Water is pooling rapidly across the floor creating a slipping hazard.",
      category: "Hostel",
      location: "Hostel Block B, 4th Floor, West Wing Restroom",
      priority: "High",
      status: "In Progress",
      assignedDepartment: "Plumbing & Civil Maintenance",
      adminRemarks: "Maintenance team dispatched. Main valve temporarily shut; pipe replacement in progress.",
      imageUrl: "https://images.unsplash.com/photo-1584622650111-993a426fbf0a?auto=format&fit=crop&w=600&q=80",
      statusHistory: [
        {
          status: "Pending",
          changedBy: studentAlex.name,
          changedByRole: "student",
          timestamp: new Date(Date.now() - 2 * 864e5).toISOString(),
          remarks: "Issue reported by student."
        },
        {
          status: "Assigned",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 1.8 * 864e5).toISOString(),
          department: "Plumbing & Civil Maintenance",
          remarks: "Assigned to Mr. Robert (Plumbing Supervisor)."
        },
        {
          status: "In Progress",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 1 * 864e5).toISOString(),
          remarks: "Technicians on site repairing the pipe fitting."
        }
      ],
      createdAt: new Date(Date.now() - 2 * 864e5).toISOString(),
      updatedAt: new Date(Date.now() - 1 * 864e5).toISOString()
    },
    {
      _id: "issue_cf_1002",
      issueId: "CF-2026-1002",
      studentId: studentAlex._id,
      studentName: studentAlex.name,
      studentEmail: studentAlex.email,
      studentDepartment: studentAlex.department,
      title: "Ceiling projector lamp blown in Lecture Hall 302",
      description: "The overhead EPSON projector turned off with a loud pop and blinking red warning indicator during Advanced Algorithms class.",
      category: "Classroom",
      location: "Academic Complex Block 2, Lecture Hall 302",
      priority: "Medium",
      status: "Resolved",
      assignedDepartment: "Academic Facilities & Audio-Visual Cell",
      adminRemarks: "Replaced optical bulb assembly and tested HDMI signal transmission. Working normally.",
      statusHistory: [
        {
          status: "Pending",
          changedBy: studentAlex.name,
          changedByRole: "student",
          timestamp: new Date(Date.now() - 5 * 864e5).toISOString()
        },
        {
          status: "Assigned",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 4.5 * 864e5).toISOString(),
          department: "Academic Facilities & Audio-Visual Cell"
        },
        {
          status: "In Progress",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 4 * 864e5).toISOString()
        },
        {
          status: "Resolved",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 3.8 * 864e5).toISOString(),
          remarks: "Bulb replaced and color calibration verified."
        }
      ],
      createdAt: new Date(Date.now() - 5 * 864e5).toISOString(),
      updatedAt: new Date(Date.now() - 3.8 * 864e5).toISOString()
    },
    {
      _id: "issue_cf_1003",
      issueId: "CF-2026-1003",
      studentId: studentPriya._id,
      studentName: studentPriya.name,
      studentEmail: studentPriya.email,
      studentDepartment: studentPriya.department,
      title: "CNC milling machine safety emergency stop switch stuck",
      description: "The red emergency cut-off button on CNC Mill #3 remains depressed and cannot be reset mechanically. Machine is offline.",
      category: "Laboratory",
      location: "Mechanical Workshop, Advanced Machining Lab B-04",
      priority: "Critical",
      status: "Assigned",
      assignedDepartment: "Laboratory Technical Support & Safety Cell",
      adminRemarks: "Machine locked out with LOTO tag. Vendor technician scheduled for inspection today.",
      statusHistory: [
        {
          status: "Pending",
          changedBy: studentPriya.name,
          changedByRole: "student",
          timestamp: new Date(Date.now() - 1 * 864e5).toISOString()
        },
        {
          status: "Assigned",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 0.8 * 864e5).toISOString(),
          department: "Laboratory Technical Support & Safety Cell",
          remarks: "Safety officer alerted. Machine isolated."
        }
      ],
      createdAt: new Date(Date.now() - 1 * 864e5).toISOString(),
      updatedAt: new Date(Date.now() - 0.8 * 864e5).toISOString()
    },
    {
      _id: "issue_cf_1004",
      issueId: "CF-2026-1004",
      studentId: studentAlex._id,
      studentName: studentAlex.name,
      studentEmail: studentAlex.email,
      studentDepartment: studentAlex.department,
      title: "High latency and frequent packet loss on Central Library Wi-Fi",
      description: 'Connecting to "Campus_Secure_5G" in 2nd floor reading room drops connection every 3 minutes. Speedtest shows 0.2 Mbps down.',
      category: "Internet",
      location: "Central Library, 2nd Floor Silent Study Wing",
      priority: "Medium",
      status: "Pending",
      assignedDepartment: "Network & IT Infrastructure Cell",
      adminRemarks: "",
      statusHistory: [
        {
          status: "Pending",
          changedBy: studentAlex.name,
          changedByRole: "student",
          timestamp: new Date(Date.now() - 0.5 * 864e5).toISOString(),
          remarks: "Issue registered by student."
        }
      ],
      createdAt: new Date(Date.now() - 0.5 * 864e5).toISOString(),
      updatedAt: new Date(Date.now() - 0.5 * 864e5).toISOString()
    },
    {
      _id: "issue_cf_1005",
      issueId: "CF-2026-1005",
      studentId: studentPriya._id,
      studentName: studentPriya.name,
      studentEmail: studentPriya.email,
      studentDepartment: studentPriya.department,
      title: "Broken electrical switchboard with exposed wires near stairwell",
      description: "Plastic faceplate broken off in building stairwell. Live copper terminals are exposed at chest height.",
      category: "Electricity",
      location: "Science Block, West Stairwell between 1st & 2nd floors",
      priority: "Critical",
      status: "In Progress",
      assignedDepartment: "Electrical Works & Power Division",
      adminRemarks: "Area cordoned off. Power circuit isolated at main DB. Replacement modular faceplate being installed.",
      statusHistory: [
        {
          status: "Pending",
          changedBy: studentPriya.name,
          changedByRole: "student",
          timestamp: new Date(Date.now() - 1.5 * 864e5).toISOString()
        },
        {
          status: "Assigned",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 1.3 * 864e5).toISOString(),
          department: "Electrical Works & Power Division"
        },
        {
          status: "In Progress",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 0.9 * 864e5).toISOString(),
          remarks: "Electrician on site with replacement materials."
        }
      ],
      createdAt: new Date(Date.now() - 1.5 * 864e5).toISOString(),
      updatedAt: new Date(Date.now() - 0.9 * 864e5).toISOString()
    },
    {
      _id: "issue_cf_1006",
      issueId: "CF-2026-1006",
      studentId: studentAlex._id,
      studentName: studentAlex.name,
      studentEmail: studentAlex.email,
      studentDepartment: studentAlex.department,
      title: "Overflowing recycle bins near cafeteria entrance",
      description: "Recycling and compost bins have exceeded capacity since yesterday afternoon; plastic cups and containers are spilling onto the walkway.",
      category: "Cleanliness",
      location: "Student Activity Center, South Plaza Cafeteria entrance",
      priority: "Low",
      status: "Resolved",
      assignedDepartment: "Sanitation & Housekeeping Division",
      adminRemarks: "Bins emptied and sanitized. Housekeeping schedule updated for extra afternoon sweep.",
      statusHistory: [
        {
          status: "Pending",
          changedBy: studentAlex.name,
          changedByRole: "student",
          timestamp: new Date(Date.now() - 6 * 864e5).toISOString()
        },
        {
          status: "Assigned",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 5.8 * 864e5).toISOString(),
          department: "Sanitation & Housekeeping Division"
        },
        {
          status: "Resolved",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 5.5 * 864e5).toISOString(),
          remarks: "Cleaned and cleared."
        }
      ],
      createdAt: new Date(Date.now() - 6 * 864e5).toISOString(),
      updatedAt: new Date(Date.now() - 5.5 * 864e5).toISOString()
    },
    {
      _id: "issue_cf_1007",
      issueId: "CF-2026-1007",
      studentId: studentPriya._id,
      studentName: studentPriya.name,
      studentEmail: studentPriya.email,
      studentDepartment: studentPriya.department,
      title: "Water cooler dispensing warm water and making humming noise",
      description: "The stainless steel drinking fountain on floor 2 is not chilling water and makes a loud vibrating noise when pressed.",
      category: "Water",
      location: "Engineering Tower B, 2nd Floor Corridor",
      priority: "Medium",
      status: "Pending",
      assignedDepartment: "Plumbing & Civil Maintenance",
      adminRemarks: "",
      statusHistory: [
        {
          status: "Pending",
          changedBy: studentPriya.name,
          changedByRole: "student",
          timestamp: new Date(Date.now() - 0.2 * 864e5).toISOString()
        }
      ],
      createdAt: new Date(Date.now() - 0.2 * 864e5).toISOString(),
      updatedAt: new Date(Date.now() - 0.2 * 864e5).toISOString()
    },
    {
      _id: "issue_cf_1008",
      issueId: "CF-2026-1008",
      studentId: studentAlex._id,
      studentName: studentAlex.name,
      studentEmail: studentAlex.email,
      studentDepartment: studentAlex.department,
      title: "Evening shuttle bus Route 4 delayed over 45 minutes consistently",
      description: "The 6:00 PM campus shuttle towards North Metro has consistently arrived after 6:45 PM all week with no driver dispatch update.",
      category: "Transport",
      location: "Main Gate Campus Transit Stop #2",
      priority: "Medium",
      status: "Assigned",
      assignedDepartment: "Campus Transport Fleet Operations",
      adminRemarks: "Coordinating with fleet manager regarding route roadworks detour.",
      statusHistory: [
        {
          status: "Pending",
          changedBy: studentAlex.name,
          changedByRole: "student",
          timestamp: new Date(Date.now() - 2.5 * 864e5).toISOString()
        },
        {
          status: "Assigned",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 2.2 * 864e5).toISOString(),
          department: "Campus Transport Fleet Operations"
        }
      ],
      createdAt: new Date(Date.now() - 2.5 * 864e5).toISOString(),
      updatedAt: new Date(Date.now() - 2.2 * 864e5).toISOString()
    },
    {
      _id: "issue_cf_1009",
      issueId: "CF-2026-1009",
      studentId: studentPriya._id,
      studentName: studentPriya.name,
      studentEmail: studentPriya.email,
      studentDepartment: studentPriya.department,
      title: "Air conditioning unit blowing warm air in East Study Hall",
      description: "East Study Hall temperature reached 29\xB0C today. The thermostat displays error code E4 and condenser unit outside is silent.",
      category: "Library",
      location: "Central Library, East Study Hall, Room 104",
      priority: "High",
      status: "In Progress",
      assignedDepartment: "Electrical Works & Power Division",
      adminRemarks: "HVAC technician diagnosed low refrigerant; pressure test currently being conducted.",
      statusHistory: [
        {
          status: "Pending",
          changedBy: studentPriya.name,
          changedByRole: "student",
          timestamp: new Date(Date.now() - 3 * 864e5).toISOString()
        },
        {
          status: "Assigned",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 2.8 * 864e5).toISOString(),
          department: "Electrical Works & Power Division"
        },
        {
          status: "In Progress",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 2 * 864e5).toISOString(),
          remarks: "HVAC team working on the rooftop condenser unit."
        }
      ],
      createdAt: new Date(Date.now() - 3 * 864e5).toISOString(),
      updatedAt: new Date(Date.now() - 2 * 864e5).toISOString()
    },
    {
      _id: "issue_cf_1010",
      issueId: "CF-2026-1010",
      studentId: studentAlex._id,
      studentName: studentAlex.name,
      studentEmail: studentAlex.email,
      studentDepartment: studentAlex.department,
      title: "Unsanitary tray return conveyor belt in Food Court",
      description: "Food residue has accumulated along the automatic roller belt at station 2, causing bad odors and attracting flies.",
      category: "Canteen",
      location: "Main Dining Commons, First Floor Dish Return Station #2",
      priority: "High",
      status: "Resolved",
      assignedDepartment: "Campus Hospitality & Food Safety Committee",
      adminRemarks: "Deep steam cleaned and food-grade disinfectant applied. Conveyor mechanism serviced.",
      statusHistory: [
        {
          status: "Pending",
          changedBy: studentAlex.name,
          changedByRole: "student",
          timestamp: new Date(Date.now() - 4 * 864e5).toISOString()
        },
        {
          status: "Assigned",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 3.8 * 864e5).toISOString(),
          department: "Campus Hospitality & Food Safety Committee"
        },
        {
          status: "Resolved",
          changedBy: "Campus Facility Administrator",
          changedByRole: "admin",
          timestamp: new Date(Date.now() - 3.2 * 864e5).toISOString(),
          remarks: "Steam sanitation completed and inspected by food hygiene officer."
        }
      ],
      createdAt: new Date(Date.now() - 4 * 864e5).toISOString(),
      updatedAt: new Date(Date.now() - 3.2 * 864e5).toISOString()
    }
  ];
  await db.issues.insertMany(sampleIssues);
  const sampleNotifications = [
    {
      _id: "notif_1",
      userId: studentAlex._id,
      message: "Your issue CF-2026-1001 (Water leaking) is now In Progress by Plumbing & Civil Maintenance.",
      type: "status_change",
      issueId: "CF-2026-1001",
      read: false,
      createdAt: new Date(Date.now() - 1 * 864e5).toISOString()
    },
    {
      _id: "notif_2",
      userId: studentAlex._id,
      message: "Your issue CF-2026-1002 (Lecture Hall 302 projector) has been Resolved!",
      type: "resolved",
      issueId: "CF-2026-1002",
      read: true,
      createdAt: new Date(Date.now() - 3.8 * 864e5).toISOString()
    },
    {
      _id: "notif_3",
      userId: studentPriya._id,
      message: "Your issue CF-2026-1003 has been Assigned to Laboratory Technical Support & Safety Cell.",
      type: "assigned",
      issueId: "CF-2026-1003",
      read: false,
      createdAt: new Date(Date.now() - 0.8 * 864e5).toISOString()
    },
    {
      _id: "notif_4",
      userId: studentPriya._id,
      message: 'Admin added remark to CF-2026-1005: "Power circuit isolated; replacement faceplate being installed."',
      type: "remark",
      issueId: "CF-2026-1005",
      read: false,
      createdAt: new Date(Date.now() - 0.9 * 864e5).toISOString()
    }
  ];
  await db.notifications.insertMany(sampleNotifications);
  console.log("Demo seed data populated successfully: 1 Admin, 2 Students, 10 Issues, 4 Notifications.");
}

// server/app.ts
import express from "express";
import cors from "cors";

// server/routes/authRoutes.ts
import { Router } from "express";

// server/controllers/authController.ts
import bcrypt2 from "bcryptjs";
import jwt2 from "jsonwebtoken";

// server/middleware/auth.ts
import jwt from "jsonwebtoken";
var JWT_SECRET = process.env.JWT_SECRET || "campusfix_super_secure_jwt_secret_key_2026";
async function authenticateToken(req, res, next) {
  const authHeader = req.headers["authorization"];
  const token = authHeader && authHeader.startsWith("Bearer ") ? authHeader.split(" ")[1] : null;
  if (!token) {
    res.status(401).json({ error: "Authentication required. Please log in." });
    return;
  }
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    const user = await db.users.findOne({ _id: decoded.id });
    if (!user) {
      res.status(401).json({ error: "User account not found. Please log in again." });
      return;
    }
    req.user = user;
    next();
  } catch (err) {
    res.status(403).json({ error: "Invalid or expired session. Please log in again." });
  }
}
function requireRole(allowedRoles) {
  return (req, res, next) => {
    if (!req.user) {
      res.status(401).json({ error: "Authentication required." });
      return;
    }
    if (!allowedRoles.includes(req.user.role)) {
      res.status(403).json({ error: "Access denied. You do not have permission for this action." });
      return;
    }
    next();
  };
}

// server/controllers/authController.ts
function formatUserResponse(user) {
  return {
    _id: user._id,
    name: user.name,
    email: user.email,
    department: user.department,
    year: user.year,
    role: user.role,
    createdAt: user.createdAt
  };
}
async function register(req, res) {
  try {
    const { name, email, password, confirmPassword, department, year } = req.body;
    if (!name || !email || !password || !confirmPassword || !department || !year) {
      res.status(400).json({ error: "Please fill all required fields." });
      return;
    }
    const trimmedEmail = email.trim().toLowerCase();
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(trimmedEmail)) {
      res.status(400).json({ error: "Please provide a valid email address." });
      return;
    }
    if (password !== confirmPassword) {
      res.status(400).json({ error: "Passwords do not match." });
      return;
    }
    if (password.length < 6) {
      res.status(400).json({ error: "Password must be at least 6 characters long." });
      return;
    }
    const existingUser = await db.users.findOne({ email: trimmedEmail });
    if (existingUser) {
      res.status(400).json({ error: "An account with this email address already exists. Please log in." });
      return;
    }
    const salt = await bcrypt2.genSalt(10);
    const hashedPassword = await bcrypt2.hash(password, salt);
    const newUser = await db.users.insertOne({
      name: name.trim(),
      email: trimmedEmail,
      password: hashedPassword,
      department: department.trim(),
      year: year.trim(),
      role: "student",
      createdAt: (/* @__PURE__ */ new Date()).toISOString()
    });
    const token = jwt2.sign(
      { id: newUser._id, email: newUser.email, role: newUser.role },
      JWT_SECRET,
      { expiresIn: "7d" }
    );
    await db.notifications.insertOne({
      userId: newUser._id,
      message: `Welcome to CampusFix, ${newUser.name}! You can now submit and track campus issues.`,
      type: "system",
      read: false,
      createdAt: (/* @__PURE__ */ new Date()).toISOString()
    });
    res.status(201).json({
      message: "Account registered successfully.",
      token,
      user: formatUserResponse(newUser)
    });
  } catch (error) {
    console.error("Registration error:", error);
    res.status(500).json({ error: "Unable to complete registration. Please try again." });
  }
}
async function login(req, res) {
  try {
    const { email, password } = req.body;
    if (!email || !password) {
      res.status(400).json({ error: "Please enter both email and password." });
      return;
    }
    const trimmedEmail = email.trim().toLowerCase();
    const user = await db.users.findOne({ email: trimmedEmail });
    if (!user) {
      res.status(401).json({ error: "Invalid email or password." });
      return;
    }
    const isMatch = await bcrypt2.compare(password, user.password);
    if (!isMatch) {
      res.status(401).json({ error: "Invalid email or password." });
      return;
    }
    const token = jwt2.sign(
      { id: user._id, email: user.email, role: user.role },
      JWT_SECRET,
      { expiresIn: "7d" }
    );
    res.status(200).json({
      message: "Logged in successfully.",
      token,
      user: formatUserResponse(user)
    });
  } catch (error) {
    console.error("Login error:", error);
    res.status(500).json({ error: "Unable to log in. Please try again." });
  }
}
async function adminLogin(req, res) {
  try {
    const { email, password } = req.body;
    if (!email || !password) {
      res.status(400).json({ error: "Please enter both email and password." });
      return;
    }
    const trimmedEmail = email.trim().toLowerCase();
    const user = await db.users.findOne({ email: trimmedEmail });
    if (!user) {
      res.status(401).json({ error: "Invalid administrator credentials." });
      return;
    }
    if (user.role !== "admin") {
      res.status(403).json({ error: "Access denied. Administrator privileges required." });
      return;
    }
    const isMatch = await bcrypt2.compare(password, user.password);
    if (!isMatch) {
      res.status(401).json({ error: "Invalid administrator credentials." });
      return;
    }
    const token = jwt2.sign(
      { id: user._id, email: user.email, role: user.role },
      JWT_SECRET,
      { expiresIn: "7d" }
    );
    res.status(200).json({
      message: "Admin access granted.",
      token,
      user: formatUserResponse(user)
    });
  } catch (error) {
    console.error("Admin login error:", error);
    res.status(500).json({ error: "Unable to log in as administrator. Please try again." });
  }
}
async function getMe(req, res) {
  try {
    if (!req.user) {
      res.status(401).json({ error: "User not authenticated." });
      return;
    }
    res.status(200).json({ user: formatUserResponse(req.user) });
  } catch (error) {
    res.status(500).json({ error: "Failed to retrieve user profile." });
  }
}
async function updateProfile(req, res) {
  try {
    if (!req.user) {
      res.status(401).json({ error: "User not authenticated." });
      return;
    }
    const { name, email, department, year } = req.body;
    if (!name || !email) {
      res.status(400).json({ error: "Name and email are required." });
      return;
    }
    const trimmedEmail = email.trim().toLowerCase();
    if (trimmedEmail !== req.user.email) {
      const existing = await db.users.findOne({ email: trimmedEmail });
      if (existing && existing._id !== req.user._id) {
        res.status(400).json({ error: "Email is already in use by another user." });
        return;
      }
    }
    const updateData = {
      name: name.trim(),
      email: trimmedEmail
    };
    if (department !== void 0) updateData.department = department.trim();
    if (year !== void 0) updateData.year = year.trim();
    await db.users.updateOne({ _id: req.user._id }, { $set: updateData });
    const updatedUser = await db.users.findOne({ _id: req.user._id });
    if (!updatedUser) {
      res.status(404).json({ error: "User not found after update." });
      return;
    }
    res.status(200).json({
      message: "Profile updated successfully.",
      user: formatUserResponse(updatedUser)
    });
  } catch (error) {
    console.error("Update profile error:", error);
    res.status(500).json({ error: "Unable to update profile. Please try again." });
  }
}

// server/routes/authRoutes.ts
var router = Router();
router.post("/register", register);
router.post("/login", login);
router.post("/admin/login", adminLogin);
router.get("/me", authenticateToken, getMe);
router.put("/profile", authenticateToken, updateProfile);
var authRoutes_default = router;

// server/routes/issueRoutes.ts
import { Router as Router2 } from "express";

// server/services/aiClassifier.ts
import { GoogleGenAI } from "@google/genai";
var VALID_CATEGORIES = [
  "Hostel",
  "Classroom",
  "Laboratory",
  "Library",
  "Canteen",
  "Transport",
  "Electricity",
  "Water",
  "Internet",
  "Cleanliness",
  "Other"
];
var VALID_PRIORITIES = ["Low", "Medium", "High", "Critical"];
function classifyWithRules(title, description) {
  const text = `${title} ${description}`.toLowerCase();
  let category = "Other";
  let priority = "Medium";
  let department = "General Administration";
  let reasoning = "Categorized using campus facility classification rules.";
  if (text.includes("water") || text.includes("leak") || text.includes("pipe") || text.includes("tap") || text.includes("drain") || text.includes("flush") || text.includes("sewage") || text.includes("overflow") || text.includes("plumb")) {
    if (text.includes("hostel") || text.includes("room") || text.includes("bathroom") || text.includes("washroom")) {
      category = "Hostel";
    } else {
      category = "Water";
    }
    department = "Plumbing & Civil Maintenance";
    reasoning = "Water, leakage, or plumbing keywords detected.";
  } else if (text.includes("electric") || text.includes("power") || text.includes("light") || text.includes("fan") || text.includes("socket") || text.includes("switch") || text.includes("short circuit") || text.includes("blackout") || text.includes("voltage") || text.includes("ac") || text.includes("air conditioner")) {
    category = "Electricity";
    department = "Electrical Works & Power Division";
    reasoning = "Electrical fixture or power supply issue identified.";
  } else if (text.includes("wifi") || text.includes("wi-fi") || text.includes("internet") || text.includes("network") || text.includes("lan") || text.includes("router") || text.includes("speed") || text.includes("connection")) {
    category = "Internet";
    department = "Network & IT Infrastructure Cell";
    reasoning = "Network connectivity or campus Wi-Fi disruption identified.";
  } else if (text.includes("garbage") || text.includes("clean") || text.includes("dirty") || text.includes("trash") || text.includes("smell") || text.includes("stink") || text.includes("pest") || text.includes("cockroach") || text.includes("waste") || text.includes("dust")) {
    category = "Cleanliness";
    department = "Sanitation & Housekeeping Division";
    reasoning = "Hygiene, trash, or sanitation requirements detected.";
  } else if (text.includes("hostel") || text.includes("warden") || text.includes("mess") || text.includes("bed") || text.includes("room") || text.includes("dorm")) {
    category = "Hostel";
    department = "Hostel Administration & Estate Management";
    reasoning = "Residential / hostel amenities issue detected.";
  } else if (text.includes("class") || text.includes("projector") || text.includes("bench") || text.includes("whiteboard") || text.includes("blackboard") || text.includes("lectern") || text.includes("audio") || text.includes("mic")) {
    category = "Classroom";
    department = "Academic Facilities & Audio-Visual Cell";
    reasoning = "Classroom instructional equipment or furniture issue detected.";
  } else if (text.includes("lab") || text.includes("apparatus") || text.includes("chemical") || text.includes("equipment") || text.includes("system") || text.includes("computer") || text.includes("multimeter") || text.includes("oscilloscope")) {
    category = "Laboratory";
    department = "Laboratory Technical Support & Safety Cell";
    reasoning = "Laboratory equipment or hardware malfunction identified.";
  } else if (text.includes("library") || text.includes("book") || text.includes("reading room") || text.includes("digital library") || text.includes("kiosk")) {
    category = "Library";
    department = "Central Library Services";
    reasoning = "Library resources or facility issue identified.";
  } else if (text.includes("canteen") || text.includes("food") || text.includes("cafeteria") || text.includes("drinking water") || text.includes("snack")) {
    category = "Canteen";
    department = "Campus Hospitality & Food Safety Committee";
    reasoning = "Canteen hygiene or food service complaint identified.";
  } else if (text.includes("bus") || text.includes("transport") || text.includes("shuttle") || text.includes("driver") || text.includes("parking") || text.includes("vehicle")) {
    category = "Transport";
    department = "Campus Transport Fleet Operations";
    reasoning = "College bus or transit schedule issue identified.";
  }
  if (text.includes("fire") || text.includes("spark") || text.includes("shock") || text.includes("electric shock") || text.includes("severe") || text.includes("emergency") || text.includes("danger") || text.includes("collapsed") || text.includes("bleeding") || text.includes("hazard")) {
    priority = "Critical";
    reasoning += " Flagged Critical due to immediate safety risk.";
  } else if (text.includes("urgent") || text.includes("broken") || text.includes("overflow") || text.includes("exam") || text.includes("leaking") || text.includes("stopped working") || text.includes("immediate")) {
    priority = "High";
    reasoning += " Flagged High due to high impact on daily student activities.";
  } else if (text.includes("minor") || text.includes("slow") || text.includes("flicker") || text.includes("sometime") || text.includes("suggestion")) {
    priority = "Low";
    reasoning += " Evaluated as Low priority non-blocking concern.";
  }
  return {
    category,
    priority,
    department,
    reasoning,
    confidence: 0.88
  };
}
async function classifyIssue(title, description, providedCategory, providedPriority) {
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey || apiKey === "MY_GEMINI_API_KEY") {
    return classifyWithRules(title, description);
  }
  try {
    const ai = new GoogleGenAI({});
    const prompt = `You are the CampusFix AI Issue Classifier for a university campus facility management system.
Analyze the following student issue report and categorize it:

Title: "${title}"
Description: "${description}"
User Selected Category (optional): "${providedCategory || "None"}"
User Selected Priority (optional): "${providedPriority || "None"}"

Allowed Categories:
["Hostel", "Classroom", "Laboratory", "Library", "Canteen", "Transport", "Electricity", "Water", "Internet", "Cleanliness", "Other"]

Allowed Priorities:
["Low", "Medium", "High", "Critical"]

Appropriate Campus Departments:
- "Plumbing & Civil Maintenance"
- "Electrical Works & Power Division"
- "Network & IT Infrastructure Cell"
- "Sanitation & Housekeeping Division"
- "Hostel Administration & Estate Management"
- "Academic Facilities & Audio-Visual Cell"
- "Laboratory Technical Support & Safety Cell"
- "Central Library Services"
- "Campus Hospitality & Food Safety Committee"
- "Campus Transport Fleet Operations"
- "General Administration"

Respond ONLY with a JSON object in this exact schema:
{
  "category": "one of the allowed categories",
  "priority": "one of the allowed priorities",
  "department": "appropriate campus department name",
  "reasoning": "brief explanation (max 20 words)",
  "confidence": 0.95
}`;
    const timeoutPromise = new Promise(
      (_, reject) => setTimeout(() => reject(new Error("AI classification timeout")), 3500)
    );
    const generatePromise = ai.models.generateContent({
      model: "gemini-3.8-flash",
      contents: prompt,
      config: {
        responseMimeType: "application/json"
      }
    });
    const response = await Promise.race([generatePromise, timeoutPromise]);
    if (response.text) {
      const parsed = JSON.parse(response.text.trim());
      const category = VALID_CATEGORIES.includes(parsed.category) ? parsed.category : "Other";
      const priority = VALID_PRIORITIES.includes(parsed.priority) ? parsed.priority : "Medium";
      const department = parsed.department || "General Administration";
      const reasoning = parsed.reasoning || "Classified by Gemini AI model.";
      const confidence = typeof parsed.confidence === "number" ? parsed.confidence : 0.95;
      return { category, priority, department, reasoning, confidence };
    }
  } catch (error) {
    console.warn("Gemini AI classification fallback triggered:", error.message);
  }
  return classifyWithRules(title, description);
}

// server/controllers/issueController.ts
var VALID_CATEGORIES2 = [
  "Hostel",
  "Classroom",
  "Laboratory",
  "Library",
  "Canteen",
  "Transport",
  "Electricity",
  "Water",
  "Internet",
  "Cleanliness",
  "Other"
];
var VALID_PRIORITIES2 = ["Low", "Medium", "High", "Critical"];
var VALID_STATUSES = ["Pending", "Assigned", "In Progress", "Resolved", "Rejected"];
async function generateIssueId() {
  const count = await db.issues.countDocuments();
  const randomSuffix = Math.floor(1e3 + Math.random() * 9e3);
  const year = (/* @__PURE__ */ new Date()).getFullYear();
  return `CF-${year}-${1e3 + count + 1}-${randomSuffix.toString().slice(0, 2)}`;
}
async function createIssue(req, res) {
  try {
    if (!req.user) {
      res.status(401).json({ error: "Authentication required." });
      return;
    }
    const { title, description, category, location, priority, imageUrl } = req.body;
    if (!title || !description || !category || !location || !priority) {
      res.status(400).json({ error: "Please fill all required fields." });
      return;
    }
    if (!VALID_CATEGORIES2.includes(category)) {
      res.status(400).json({ error: "Invalid category selected." });
      return;
    }
    if (!VALID_PRIORITIES2.includes(priority)) {
      res.status(400).json({ error: "Invalid priority level selected." });
      return;
    }
    const aiAnalysis = await classifyIssue(title, description, category, priority);
    const assignedDepartment = aiAnalysis.department || "General Administration";
    const issueId = await generateIssueId();
    const now = (/* @__PURE__ */ new Date()).toISOString();
    const statusHistory = [
      {
        status: "Pending",
        changedBy: req.user.name,
        changedByRole: req.user.role,
        timestamp: now,
        remarks: "Issue submitted by student."
      }
    ];
    const newIssue = await db.issues.insertOne({
      issueId,
      studentId: req.user._id,
      studentName: req.user.name,
      studentEmail: req.user.email,
      studentDepartment: req.user.department,
      title: title.trim(),
      description: description.trim(),
      category,
      location: location.trim(),
      priority,
      status: "Pending",
      assignedDepartment,
      adminRemarks: "",
      imageUrl: imageUrl?.trim() || void 0,
      statusHistory,
      createdAt: now,
      updatedAt: now
    });
    await db.notifications.insertOne({
      userId: req.user._id,
      message: `Your issue ${issueId} ("${title.slice(0, 30)}...") has been submitted and queued for review.`,
      type: "submission",
      issueId,
      read: false,
      createdAt: now
    });
    res.status(201).json({
      message: "Issue submitted successfully.",
      issue: newIssue
    });
  } catch (error) {
    console.error("Create issue error:", error);
    res.status(500).json({ error: "Unable to submit issue. Please try again." });
  }
}
async function getMyIssues(req, res) {
  try {
    if (!req.user) {
      res.status(401).json({ error: "Authentication required." });
      return;
    }
    const issues = await db.issues.find({ studentId: req.user._id });
    issues.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());
    res.status(200).json({ issues });
  } catch (error) {
    console.error("Get my issues error:", error);
    res.status(500).json({ error: "Failed to retrieve your issues." });
  }
}
async function getAllIssues(req, res) {
  try {
    const { search, category, priority, status } = req.query;
    let issues = await db.issues.find();
    if (category && category !== "All") {
      issues = issues.filter((i) => i.category === category);
    }
    if (priority && priority !== "All") {
      issues = issues.filter((i) => i.priority === priority);
    }
    if (status && status !== "All") {
      issues = issues.filter((i) => i.status === status);
    }
    if (search && typeof search === "string") {
      const q = search.toLowerCase();
      issues = issues.filter(
        (i) => i.title.toLowerCase().includes(q) || i.issueId.toLowerCase().includes(q) || i.location.toLowerCase().includes(q) || i.studentName.toLowerCase().includes(q) || i.assignedDepartment.toLowerCase().includes(q)
      );
    }
    issues.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());
    res.status(200).json({ issues });
  } catch (error) {
    console.error("Get all issues error:", error);
    res.status(500).json({ error: "Failed to fetch issues." });
  }
}
async function getIssueById(req, res) {
  try {
    const { id } = req.params;
    let issue = await db.issues.findOne({ _id: id });
    if (!issue) {
      issue = await db.issues.findOne({ issueId: id });
    }
    if (!issue) {
      res.status(404).json({ error: "Issue not found." });
      return;
    }
    if (req.user && req.user.role !== "admin" && issue.studentId !== req.user._id) {
      res.status(403).json({ error: "Access denied to this issue record." });
      return;
    }
    res.status(200).json({ issue });
  } catch (error) {
    res.status(500).json({ error: "Failed to retrieve issue details." });
  }
}
async function trackIssue(req, res) {
  try {
    const { issueId } = req.params;
    if (!issueId) {
      res.status(400).json({ error: "Issue ID is required." });
      return;
    }
    const trimmed = issueId.trim().toUpperCase();
    const issue = await db.issues.findOne({ issueId: trimmed });
    if (!issue) {
      res.status(404).json({ error: `No issue found matching reference ID "${trimmed}".` });
      return;
    }
    res.status(200).json({ issue });
  } catch (error) {
    res.status(500).json({ error: "Failed to track issue." });
  }
}
async function updateIssue(req, res) {
  try {
    if (!req.user || req.user.role !== "admin") {
      res.status(403).json({ error: "Administrator access required." });
      return;
    }
    const { id } = req.params;
    const { status, assignedDepartment, adminRemarks } = req.body;
    const issue = await db.issues.findOne({ _id: id });
    if (!issue) {
      res.status(404).json({ error: "Issue not found." });
      return;
    }
    const now = (/* @__PURE__ */ new Date()).toISOString();
    const isStatusChanged = status && status !== issue.status;
    const newStatus = status && VALID_STATUSES.includes(status) ? status : issue.status;
    const newDepartment = assignedDepartment !== void 0 ? assignedDepartment.trim() : issue.assignedDepartment;
    const newRemarks = adminRemarks !== void 0 ? adminRemarks.trim() : issue.adminRemarks;
    const statusHistory = [...issue.statusHistory || []];
    if (isStatusChanged || newRemarks && newRemarks !== issue.adminRemarks || newDepartment && newDepartment !== issue.assignedDepartment) {
      statusHistory.push({
        status: newStatus,
        changedBy: req.user.name,
        changedByRole: "admin",
        timestamp: now,
        remarks: newRemarks || void 0,
        department: newDepartment || void 0
      });
    }
    await db.issues.updateOne(
      { _id: id },
      {
        $set: {
          status: newStatus,
          assignedDepartment: newDepartment,
          adminRemarks: newRemarks,
          statusHistory,
          updatedAt: now
        }
      }
    );
    let notifMessage = "";
    let notifType = "status_change";
    if (newStatus === "Resolved" && issue.status !== "Resolved") {
      notifMessage = `Your issue ${issue.issueId} has been resolved! Remarks: "${newRemarks || "Completed"}".`;
      notifType = "resolved";
    } else if (newStatus === "Rejected" && issue.status !== "Rejected") {
      notifMessage = `Your issue ${issue.issueId} was rejected. Remarks: "${newRemarks || "Not applicable"}".`;
      notifType = "status_change";
    } else if (newDepartment !== issue.assignedDepartment) {
      notifMessage = `Your issue ${issue.issueId} was assigned to ${newDepartment}.`;
      notifType = "assigned";
    } else if (isStatusChanged) {
      notifMessage = `Status of issue ${issue.issueId} changed from ${issue.status} to ${newStatus}.`;
      notifType = "status_change";
    } else if (newRemarks && newRemarks !== issue.adminRemarks) {
      notifMessage = `Admin added remark on issue ${issue.issueId}: "${newRemarks}".`;
      notifType = "remark";
    }
    if (notifMessage) {
      await db.notifications.insertOne({
        userId: issue.studentId,
        message: notifMessage,
        type: notifType,
        issueId: issue.issueId,
        read: false,
        createdAt: now
      });
    }
    const updated = await db.issues.findOne({ _id: id });
    res.status(200).json({
      message: "Issue updated successfully.",
      issue: updated
    });
  } catch (error) {
    console.error("Update issue error:", error);
    res.status(500).json({ error: "Failed to update issue." });
  }
}
async function getAdminStats(req, res) {
  try {
    const issues = await db.issues.find();
    const totalIssues = issues.length;
    const pending = issues.filter((i) => i.status === "Pending").length;
    const assigned = issues.filter((i) => i.status === "Assigned").length;
    const inProgress = issues.filter((i) => i.status === "In Progress").length;
    const resolved = issues.filter((i) => i.status === "Resolved").length;
    const rejected = issues.filter((i) => i.status === "Rejected").length;
    const critical = issues.filter((i) => i.priority === "Critical").length;
    const categoryCounts = {};
    for (const c of VALID_CATEGORIES2) categoryCounts[c] = 0;
    for (const i of issues) {
      categoryCounts[i.category] = (categoryCounts[i.category] || 0) + 1;
    }
    const priorityCounts = {
      Low: 0,
      Medium: 0,
      High: 0,
      Critical: 0
    };
    for (const i of issues) {
      priorityCounts[i.priority] = (priorityCounts[i.priority] || 0) + 1;
    }
    const statusCounts = {
      Pending: pending,
      Assigned: assigned,
      "In Progress": inProgress,
      Resolved: resolved,
      Rejected: rejected
    };
    const resolutionRate = totalIssues > 0 ? Math.round(resolved / totalIssues * 100) : 0;
    res.status(200).json({
      stats: {
        totalIssues,
        pending,
        assigned,
        inProgress,
        resolved,
        rejected,
        critical,
        resolutionRate
      },
      categoryCounts,
      priorityCounts,
      statusCounts
    });
  } catch (error) {
    console.error("Stats error:", error);
    res.status(500).json({ error: "Failed to generate admin statistics." });
  }
}
async function getStudentStats(req, res) {
  try {
    if (!req.user) {
      res.status(401).json({ error: "Authentication required." });
      return;
    }
    const issues = await db.issues.find({ studentId: req.user._id });
    const totalIssues = issues.length;
    const pending = issues.filter((i) => i.status === "Pending").length;
    const assigned = issues.filter((i) => i.status === "Assigned").length;
    const inProgress = issues.filter((i) => i.status === "In Progress").length;
    const resolved = issues.filter((i) => i.status === "Resolved").length;
    const rejected = issues.filter((i) => i.status === "Rejected").length;
    res.status(200).json({
      totalIssues,
      pending,
      assigned,
      inProgress,
      resolved,
      rejected
    });
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch student statistics." });
  }
}

// server/routes/issueRoutes.ts
var router2 = Router2();
router2.get("/track/:issueId", trackIssue);
router2.post("/", authenticateToken, createIssue);
router2.get("/my", authenticateToken, getMyIssues);
router2.get("/my/stats", authenticateToken, getStudentStats);
router2.get("/all", authenticateToken, requireRole(["admin"]), getAllIssues);
router2.get("/admin/stats", authenticateToken, requireRole(["admin"]), getAdminStats);
router2.put("/:id", authenticateToken, requireRole(["admin"]), updateIssue);
router2.get("/:id", authenticateToken, getIssueById);
var issueRoutes_default = router2;

// server/routes/notificationRoutes.ts
import { Router as Router3 } from "express";

// server/controllers/notificationController.ts
async function getMyNotifications(req, res) {
  try {
    if (!req.user) {
      res.status(401).json({ error: "Authentication required." });
      return;
    }
    const notifications = await db.notifications.find({ userId: req.user._id });
    notifications.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());
    const unreadCount = notifications.filter((n) => !n.read).length;
    res.status(200).json({ notifications, unreadCount });
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch notifications." });
  }
}
async function markAsRead(req, res) {
  try {
    if (!req.user) {
      res.status(401).json({ error: "Authentication required." });
      return;
    }
    const { id } = req.params;
    await db.notifications.updateOne({ _id: id, userId: req.user._id }, { $set: { read: true } });
    res.status(200).json({ message: "Marked as read." });
  } catch (error) {
    res.status(500).json({ error: "Failed to update notification." });
  }
}
async function markAllAsRead(req, res) {
  try {
    if (!req.user) {
      res.status(401).json({ error: "Authentication required." });
      return;
    }
    const notifications = await db.notifications.find({ userId: req.user._id });
    for (const n of notifications) {
      if (!n.read) {
        await db.notifications.updateOne({ _id: n._id }, { $set: { read: true } });
      }
    }
    res.status(200).json({ message: "All notifications marked as read." });
  } catch (error) {
    res.status(500).json({ error: "Failed to mark all as read." });
  }
}

// server/routes/notificationRoutes.ts
var router3 = Router3();
router3.use(authenticateToken);
router3.get("/", getMyNotifications);
router3.put("/:id/read", markAsRead);
router3.put("/read-all", markAllAsRead);
var notificationRoutes_default = router3;

// server/routes/aiRoutes.ts
import { Router as Router4 } from "express";

// server/controllers/aiController.ts
async function handleAIClassify(req, res) {
  try {
    const { title, description, category, priority } = req.body;
    if (!title && !description) {
      res.status(400).json({ error: "Please provide an issue title or description for classification." });
      return;
    }
    const result = await classifyIssue(title || "", description || "", category, priority);
    res.status(200).json({ result });
  } catch (error) {
    console.error("AI classify error:", error);
    res.status(500).json({ error: "Failed to generate AI suggestions." });
  }
}

// server/routes/aiRoutes.ts
var router4 = Router4();
router4.post("/classify", authenticateToken, handleAIClassify);
var aiRoutes_default = router4;

// server/app.ts
function createExpressApp() {
  const app = express();
  app.use(cors());
  app.use(express.json({ limit: "10mb" }));
  app.use(express.urlencoded({ extended: true, limit: "10mb" }));
  app.use("/api/auth", authRoutes_default);
  app.use("/api/issues", issueRoutes_default);
  app.use("/api/notifications", notificationRoutes_default);
  app.use("/api/ai", aiRoutes_default);
  app.get("/api/health", (_req, res) => {
    res.json({
      status: "healthy",
      app: "CampusFix API",
      timestamp: (/* @__PURE__ */ new Date()).toISOString()
    });
  });
  app.use((err, _req, res, _next) => {
    console.error("Unhandled server error:", err);
    res.status(500).json({
      error: "An unexpected server error occurred. Please try again.",
      details: process.env.NODE_ENV === "development" ? err.message : void 0
    });
  });
  return app;
}

// server.ts
import dotenv from "dotenv";
dotenv.config();
var isProd = process.env.NODE_ENV === "production";
var PORT = isProd ? parseInt(process.env.PORT || "8080", 10) : 3e3;
async function startServer() {
  await db.init();
  await seedDemoData();
  const app = createExpressApp();
  if (!isProd) {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa"
    });
    app.use(vite.middlewares);
    console.log("Vite middleware mounted in development mode");
  } else {
    const distPath = path2.resolve(process.cwd(), "dist");
    if (fs2.existsSync(distPath)) {
      app.use(express2.static(distPath));
      app.get("*", (_req, res) => {
        res.sendFile(path2.join(distPath, "index.html"));
      });
    } else {
      console.warn("Production mode enabled but dist directory not found. Please build the client.");
    }
  }
  app.listen(PORT, "0.0.0.0", () => {
    console.log(`CampusFix server running on http://0.0.0.0:${PORT}`);
  });
}
startServer().catch((err) => {
  console.error("Fatal error starting CampusFix server:", err);
  process.exit(1);
});
