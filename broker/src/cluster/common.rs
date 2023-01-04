

use enumflags2::bitflags;

#[bitflags]
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flag { 
    // Wait4Conn   = 0b00000001,   // 等待连接成功
    // UpdateNode  = 0b00000010,   // 更新 Node
    Shutdown    = 0b00000100,   // shutdown 请求
    Fininshed   = 0b00001000,   // task 已结束
}

