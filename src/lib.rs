extern crate libc;
use libc::{c_int,c_char,free};

#[repr(C)] pub struct TCBDB;
#[repr(C)] pub struct TCLIST;

pub const BDBOREADER: c_int = 1 << 0; 
pub const BDBOWRITER: c_int = 1 << 1; 
pub const BDBOCREAT: c_int = 1 << 2;
pub const BDBOTRUNC: c_int = 1 << 3;

pub const BDBTLARGE: u8 = 1 << 0;
pub const BDBTDEFLATE: u8 = 1 << 1;
pub const BDBTBZIP: u8 = 1 << 2;
pub const BDBTTCBS: u8 = 1 << 3;

#[link(name = "tokyocabinet", kind = "static")]
extern "C" {
	/* db functions */
	pub fn tcbdbnew() -> *mut TCBDB;
	pub fn tcbdbdel(db: *mut TCBDB);
	pub fn tcbdbopen(db: *mut TCBDB, path: *const c_char, flags: c_int) -> c_int;
	pub fn tcbdbclose(db: *mut TCBDB) -> c_int;
	pub fn tcbdbsync(db: *mut TCBDB) -> c_int;
	pub fn tcbdbtune(db: *mut TCBDB, lmemb: i32, nmemb: i32, bnum: i64, apow: i8, fpow: i8, opts: u8);
	pub fn tcbdboptimize(db: *mut TCBDB, lmemb: i32, nmemb: i32, bnum: i64, apow: i8, fpow: i8, opts: u8);
	pub fn tcbdbputdup(db: *mut TCBDB, key: *const u8, ksize: c_int, val: *const u8, vsize: c_int) -> c_int;
	pub fn tcbdbget4(db: *mut TCBDB, key: *const u8, ksize: c_int) -> *mut TCLIST;
	/* list functions */
	pub fn tclistpop(list: *mut TCLIST, sz: *mut c_int) -> *mut u8;
	pub fn tclistdel(list: *mut TCLIST);
	pub fn tclistclear(list: *mut TCLIST);
}

pub mod safe {
	use std::path::Path;
	use super::*;
	use libc::{c_int,c_void,free};
	use std::ptr;
	use std::mem;
	use std::slice;
	use std::ops::Deref;
	use std::marker::PhantomData;

	pub struct List<'a,T: 'a> {
		raw: *mut TCLIST,
		phantom: PhantomData<&'a T>
	}

	pub struct Elem<'a, T: 'a> {
		raw: *mut c_void,
		size: usize,
		phantom: PhantomData<&'a T>
	}


	impl<'a,T> Drop for Elem<'a,T> {
		fn drop(&mut self) {
			unsafe { if !self.raw.is_null() { free(self.raw) } }
		}
	}

	impl<'a,T> Elem<'a,T> {
		pub fn as_slice(&self) -> &'a [u8] {
			unsafe { slice::from_raw_parts(self.raw as *const u8, self.size) }
		}
	}

	pub struct IntoIter<'a,T: 'a> {
		raw: *mut TCLIST,
		phantom: PhantomData<&'a T>
	}

	impl<'a> Iterator for IntoIter<'a,Elem<'a, &'a [u8]>> {
		type Item = Elem<'a, &'a [u8]>;

		fn next(&mut self) -> Option<Self::Item> {
			if self.raw.is_null() {
				return None;
			}
			let mut size = 0;
			let p = unsafe { tclistpop(self.raw, &mut size) };
			if p.is_null() {
				return None;
			} else {
				return Some(Elem { raw: p as *mut c_void, size: size as usize, phantom: PhantomData })
			}
		}
	}

	impl<'a> IntoIterator for List<'a,Elem<'a, &'a [u8]>> {
		type Item = Elem<'a, &'a [u8]>;
		type IntoIter = IntoIter<'a, Elem<'a, &'a [u8]>>;

		fn into_iter(self) -> Self::IntoIter {
			let i = IntoIter { raw: self.raw, phantom: PhantomData };
			mem::forget(self);
			i
		}
	}

	pub struct Bdb {
		raw: *mut TCBDB,
	}
	impl Bdb {
		pub fn new() -> Bdb {
			Bdb { raw: unsafe { tcbdbnew() } }
		}

		pub fn open(&mut self, path: &Path, flags: i32) {
			use std::ffi::CString;
			let cpath = CString::new(path.to_str().unwrap()).unwrap().as_ptr();
			let rv = unsafe { tcbdbopen(self.raw, cpath, flags as c_int) };
		}

		pub fn put_dup(&mut self, key: &[u8], val: &[u8]) {
			unsafe { tcbdbputdup(self.raw, key.as_ptr(), key.len() as i32, val.as_ptr(), val.len() as i32) };
		}

		pub fn get_list<'a>(&mut self, key: &[u8]) -> List<Elem<&'a [u8]>> {
			List { raw: unsafe { tcbdbget4(self.raw, key.as_ptr(), key.len() as i32) }, phantom: PhantomData }
		}

		pub fn sync(&mut self) {
			unsafe { tcbdbsync(self.raw) };
		}

		pub fn close(&mut self) {
			unsafe { tcbdbclose(self.raw) };
		}

		pub fn tune(&mut self, opts: u8) {
			unsafe { tcbdbtune(self.raw, 0, 0, 0, -1, -1, opts) };
		}

		pub fn optimize(&mut self) {
			unsafe { tcbdboptimize(self.raw, 0, 0, 0, -1, -1, 255) };
		}
	}

	impl Drop for Bdb {
		fn drop(&mut self) {
			unsafe { tcbdbdel(self.raw) }
		}
	}

	impl<'a,T> Drop for List<'a,T> {
		fn drop(&mut self) {
			unsafe { if !self.raw.is_null() { tclistclear(self.raw); tclistdel(self.raw) } }
		}
	}

	impl<'a,T> Drop for IntoIter<'a,T> {
		fn drop(&mut self) {
			unsafe { if !self.raw.is_null() { tclistclear(self.raw); tclistdel(self.raw) } }
		}
	}
}

#[test]
fn it_is_safe() {
	use safe::Bdb;
	use std::path::Path;
	let mut db = Bdb::new();
	db.open(Path::new("test.db"), BDBOWRITER | BDBOCREAT | BDBOTRUNC);
	db.put_dup("nigger".as_bytes(), "please".as_bytes());
	for i in db.get_list("nigger".as_bytes()) {
		println!("{:?}", String::from(i.as_slice()));
	}
}
