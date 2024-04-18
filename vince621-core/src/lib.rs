#![feature(round_char_boundary)]
#![feature(new_uninit)]
#![feature(try_trait_v2)]
#![feature(ptr_sub_ptr)]
use core::num::NonZeroU32;
use std::str::FromStr;

pub mod db;
pub mod search;
