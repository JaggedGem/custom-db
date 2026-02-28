import { describe, it, expect, beforeEach, afterEach } from "vitest";
import fs from "fs";
import path from "path";

import {
  initDatabase,
  allocatePage,
  loadPage,
  getLatestPage,
  createBitmapPage,
  createFixedPage,
  createSlottedPage,
  initChainPage,
  createColumn,
  createTable,
  PAGE_SIZE
} from "../src/index";

const TEST_DB = path.join(__dirname, "test.cdb");

beforeEach(() => {
  if (fs.existsSync(TEST_DB)) fs.unlinkSync(TEST_DB);
});

afterEach(() => {
  if (fs.existsSync(TEST_DB)) fs.unlinkSync(TEST_DB);
});

describe("initDatabase", () => {
  it("writes correct header", () => {
    const fd = initDatabase(TEST_DB, true);
    const page0 = loadPage(fd, 0);

    expect(page0.toString("utf8", 0, 3)).toBe("CDB");
    expect(page0.readUInt8(4)).toBe(1);
    expect(page0.readUInt16LE(5)).toBe(PAGE_SIZE);
    expect(page0.readUInt32LE(7)).toBe(2);

    fs.closeSync(fd);
  });

  it("creates catalog table page", () => {
    const fd = initDatabase(TEST_DB, true);
    const page1 = loadPage(fd, 1);

    expect(page1.readUInt32LE(0)).toBe(0);
    expect(page1.readUInt16LE(4)).toBe(16);
    expect(page1.readUInt16LE(6)).toBe(0);
    expect(page1.readUInt8(15)).toBe(1);

    fs.closeSync(fd);
  });
});

describe("allocatePage", () => {
  it("allocates new page and increments header", () => {
    const fd = initDatabase(TEST_DB, true);

    const pageId = allocatePage(fd);
    expect(pageId).toBe(2);

    const header = loadPage(fd, 0);
    expect(header.readUInt32LE(7)).toBe(3);

    fs.closeSync(fd);
  });
});

describe("loadPage", () => {
  it("reads written data", () => {
    const fd = initDatabase(TEST_DB, true);
    const buf = Buffer.alloc(PAGE_SIZE);
    buf.write("HELLO");

    fs.writeSync(fd, buf, 0, PAGE_SIZE, PAGE_SIZE * 2);

    const loaded = loadPage(fd, 2);
    expect(loaded.toString("utf8", 0, 5)).toBe("HELLO");

    fs.closeSync(fd);
  });
});

describe("getLatestPage", () => {
  it("returns last page in chain", () => {
    const fd = initDatabase(TEST_DB, true);

    const p1 = allocatePage(fd);
    const p2 = allocatePage(fd);

    const page = loadPage(fd, p1);
    page.writeUInt32LE(p2, 0);
    fs.writeSync(fd, page, 0, PAGE_SIZE, p1 * PAGE_SIZE);

    const res = getLatestPage(fd, p1);
    expect(res.pageId).toBe(p2);

    fs.closeSync(fd);
  });
});

describe("createBitmapPage", () => {
  it("creates bitmap page", () => {
    const fd = initDatabase(TEST_DB, true);
    const pageId = createBitmapPage(fd);
    const page = loadPage(fd, pageId);

    expect(page.readUInt8(4)).toBe(5);
    expect(page.readUInt32LE(8)).toBe(0);

    fs.closeSync(fd);
  });
});

describe("createFixedPage", () => {
  it("creates fixed page", () => {
    const fd = initDatabase(TEST_DB, true);
    const pageId = createFixedPage(fd);
    const page = loadPage(fd, pageId);

    expect(page.readUInt8(15)).toBe(4);

    fs.closeSync(fd);
  });
});

describe("createSlottedPage", () => {
  it("creates slotted page", () => {
    const fd = initDatabase(TEST_DB, true);
    const pageId = createSlottedPage(fd);
    const page = loadPage(fd, pageId);

    expect(page.readUInt16LE(4)).toBe(16);
    expect(page.readUInt16LE(6)).toBe(0);
    expect(page.readUInt8(15)).toBe(3);

    fs.closeSync(fd);
  });
});

describe("initChainPage", () => {
  it("initializes chain page correctly", () => {
    const fd = initDatabase(TEST_DB, true);
    const pid = allocatePage(fd);

    const page = initChainPage(fd, pid, 2);
    expect(page.readUInt32LE(0)).toBe(0);
    expect(page.readUInt8(15)).toBe(2);

    fs.closeSync(fd);
  });
});

describe("createColumn", () => {
  it("creates normal column", () => {
    const fd = initDatabase(TEST_DB, true);

    const colPage = allocatePage(fd);
    initChainPage(fd, colPage, 2);

    const dataPage = createColumn(fd, colPage, {
      name: "age",
      type: "integer",
      isForeignKey: false
    });

    expect(typeof dataPage).toBe("number");

    fs.closeSync(fd);
  });

  it("creates foreign key column", () => {
    const fd = initDatabase(TEST_DB, true);

    const colPage = allocatePage(fd);
    initChainPage(fd, colPage, 2);

    const dataPage = createColumn(fd, colPage, {
      name: "user_id",
      isForeignKey: true,
      foreignKey: {
        table: "users",
        column: "id"
      }
    });

    const page = loadPage(fd, colPage);
    const type = page.readUInt8(16 + 32);
    expect(type).toBe(4);

    fs.closeSync(fd);
  });
});

describe("createTable", () => {
  it("creates table with columns", () => {
    const fd = initDatabase(TEST_DB, true);

    createTable(fd, "users", [
      { name: "id", type: "integer", isForeignKey: false },
      { name: "name", type: "string", isForeignKey: false }
    ]);

    const page1 = loadPage(fd, 1);
    expect(page1.readUInt16LE(6)).toBe(1);

    fs.closeSync(fd);
  });

  it("rejects long table names", () => {
    const fd = initDatabase(TEST_DB, true);

    expect(() =>
      createTable(fd, "this_name_is_way_too_long", [])
    ).toThrow();

    fs.closeSync(fd);
  });
});