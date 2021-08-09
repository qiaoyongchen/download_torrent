package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/qiaoyongchen/bencode"
)

func main() {
	bc := bencode.NewBenCode("bc")
	f, e := os.Open("./debian.torrent")
	if e != nil {
		panic(e)
	}
	bts, ee := ioutil.ReadAll(f)
	if ee != nil {
		panic(ee)
	}

	torr := Torrent{}
	bc.Decode(string(bts), &torr)

	torr.Download("./a.iso")
}

const Port int = 6881

// 种子文件解析出来的结构
type Torrent struct {
	// 原始字段
	Announce     string `bc:"announce"`
	Comment      string `bc:"comment"`
	CreationDate int    `bc:"creation date"`
	Info         struct {
		Name        string `bc:"name"`         // 下载数据的名称
		Length      int    `bc:"length"`       // 下载数据的总长度
		PieceLength int    `bc:"piece length"` // 每个数据块的长度
		Pieces      string `bc:"pieces"`       // 所有数据块hash合在一起的hash
	} `bc:"info"`

	// 计算出来的字段
	Peers       []peer     // 可用节点
	PeerID      [20]byte   // 随机生成代表当前节点的 id
	InfoHash    [20]byte   // info 字段计算出的 hash 值
	PieceHashes [][20]byte // 每个数据块的hash值
}

func (torr *Torrent) Download(tofile string) error {
	log.Printf("tracker 服务器地址：%s", torr.Announce)

	// 验证种子信息的正确性
	if !torr.validate() {
		log.Println("错误的种子文件信息: ", torr.Info.Name)
		return errors.New("错误的种子文件信息")
	}

	// 随即生成一个 peerid ， 该 peerid 代表当前节点
	var randPeerId [20]byte
	rand.Read(randPeerId[:])

	// 获取 peers
	peers, err := torr.getPeers(randPeerId, Port)
	if err != nil {
		log.Println("获取peer出错: ", err)
		return err
	}

	// 解析数据
	torr.Peers = peers
	torr.PeerID = randPeerId
	torr.InfoHash = torr.getInfoHash()
	torr.PieceHashes = torr.getPieceHashs()

	log.Println("开始下载: ", torr.Info.Name)

	workCh := make(chan *pieceWork, len(torr.PieceHashes)) // TODO len(torr.PieceHashes)
	resultCh := make(chan *pieceResult)

	// 每个片段分配一个下载任务，放到任务队列中
	for index, hash := range torr.PieceHashes {
		length := torr.calculatePieceSize(index)
		workCh <- &pieceWork{index, hash, length}
	}

	fmt.Printf("已分发任务 [%d] 个 \n", len(workCh))

	// 每个节点并发执行下载任务，从 workCh 中获取，下载结果放入 resultCh 中
	fmt.Printf("[%d] 个下载节点 \n", len(torr.Peers))
	for _, peer := range torr.Peers {
		go torr.startDownloadWorker(peer, workCh, resultCh)
	}

	fmt.Println("已分配任务")

	buf := make([]byte, torr.Info.Length)
	donePiecesNum := 0 // 已下载完成的数据块的数量

	for donePiecesNum < len(torr.PieceHashes) {
		// 从结果队列中接收已下载完成的结果
		// 计算出该数据块在整个数据中占的字节起点和终点位置并拷贝
		res := <-resultCh
		begin, end := torr.calculatePieceBounds(res.index)
		copy(buf[begin:end], res.buf)
		donePiecesNum++

		percent := float64(donePiecesNum) / float64(len(torr.PieceHashes)) * 100
		log.Printf("(%0.2f%%) 下载数据块 [#%d], 已下载 %d / %d 块 \n", percent, res.index, donePiecesNum, len(torr.PieceHashes))
	}
	close(workCh)

	outFile, err := os.Create(tofile)
	if err != nil {
		return err
	}
	defer outFile.Close()

	if _, err = outFile.Write(buf); err != nil {
		return err
	}
	return nil
}

// 计算数据块大小 ** 最后一块可能有意外情况
func (torr *Torrent) calculatePieceSize(index int) int {
	begin, end := torr.calculatePieceBounds(index)
	return end - begin
}

// 计算每个数据块的起始和结束的边界
// 因为整个数据大小不一定为数据块大小的整数倍，如果是最后一块，需要截取结束位置
// 除了最后一个数据块，其他每个数据块的长度是固定的
// 根据 index 不难算出该数据块再整个下载数据块中的开始结束位置
func (torr *Torrent) calculatePieceBounds(index int) (begin int, end int) {
	begin = index * torr.Info.PieceLength
	end = begin + torr.Info.PieceLength

	// 最后一块数据超出总长度按总长度算
	if end > torr.Info.Length {
		end = torr.Info.Length
	}
	return begin, end
}

// 开启下载任务
func (torr *Torrent) startDownloadWorker(p peer, workch chan *pieceWork, resultch chan *pieceResult) {
	c, err := new_client(p, torr.PeerID, torr.InfoHash)
	if err != nil {
		log.Printf("握手失败: %s, 原因: %s. ", p.String(), err.Error())
		return
	}
	defer c.Conn.Close()

	log.Printf("完成握手: %s\n", p.String())

	// TODO
	c.sendUnchoke()
	c.sendInterested()

	for pw := range workch {
		log.Printf("peer [%s] 收到下载任务, 下载第 [%d] 个任务", p.String(), pw.index)

		// 如果该数据块还未下载
		// torrent 中用 1 位表示 1 个数据块是否已下载
		if !c.Bitfield.hasPiece(pw.index) {
			workch <- pw
			continue
		}

		// 下载
		buf, err := downloadPiece(c, pw)
		if err != nil {
			log.Println("遇到错误，退出: ", err)
			workch <- pw
			return
		}

		var check_integrity = func(pw *pieceWork, buf []byte) error {
			hash := sha1.Sum(buf)
			if !bytes.Equal(hash[:], pw.hash[:]) {
				return fmt.Errorf("数据块校验失败: 数据块 [%d]", pw.index)
			}
			return nil
		}

		err = check_integrity(pw, buf)
		if err != nil {
			log.Println(err.Error())
			workch <- pw
			continue
		}

		c.sendHave(pw.index)
		resultch <- &pieceResult{pw.index, buf}
	}
}

// MaxBlockSize is the largest number of bytes a request can ask for
const MaxBlockSize = 16384

// MaxBacklog is the number of unfulfilled requests a client can have in its pipeline
const MaxBacklog = 5

func downloadPiece(c *client, pw *pieceWork) ([]byte, error) {
	state := piece_progress{
		index:  pw.index,
		client: c,
		buf:    make([]byte, pw.length),
	}

	c.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	defer c.Conn.SetDeadline(time.Time{}) // Disable the deadline

	for state.downloaded < pw.length {

		if !state.client.Choked {
			for state.backlog < MaxBacklog && state.requested < pw.length {
				blockSize := MaxBlockSize

				if pw.length-state.requested < blockSize {
					blockSize = pw.length - state.requested
				}

				err := c.sendRequest(pw.index, state.requested, blockSize)
				if err != nil {
					return nil, err
				}
				state.backlog++
				state.requested += blockSize
			}
		}

		err := state.readMessage()
		if err != nil {
			return nil, err
		}
	}

	return state.buf, nil
}

type (
	pieceWork struct {
		index  int
		hash   [20]byte
		length int
	}

	pieceResult struct {
		index int
		buf   []byte
	}
)

type piece_progress struct {
	index      int
	client     *client
	buf        []byte
	downloaded int
	requested  int
	backlog    int
}

func (state *piece_progress) readMessage() error {
	msg, err := state.client.read()
	if err != nil {
		return err
	}

	if msg == nil { // keep-alive
		return nil
	}

	switch msg.ID {
	case MsgUnchoke:
		state.client.Choked = false
	case MsgChoke:
		state.client.Choked = true
	case MsgHave:
		index, err := parse_have_message(msg)
		if err != nil {
			return err
		}
		state.client.Bitfield.set_piece(index)
	case MsgPiece:
		n, err := parse_piece_message(state.index, state.buf, msg)
		if err != nil {
			return err
		}
		state.downloaded += n
		state.backlog--
	}
	return nil
}

// 验证种子文件是否正确
func (torr *Torrent) validate() bool {
	//fmt.Println(torr.Info.Length/torr.Info.PieceLength*20, len(torr.Info.Pieces), torr.Info.PieceLength)
	if torr.Info.Length%torr.Info.PieceLength == 0 {
		return torr.Info.Length/torr.Info.PieceLength*20 == len(torr.Info.Pieces)
	}
	return torr.Info.Length/torr.Info.PieceLength*20+20 == len(torr.Info.Pieces)
}

// Info 字段hash 后的值用于请求tracker服务器获取peer
func (torr *Torrent) getInfoHash() [20]byte {
	bc := bencode.NewBenCode("bc")
	info_str, info_err := bc.Encode(torr.Info)
	if info_err != nil {
		return [20]byte{}
	}
	return sha1.Sum([]byte(info_str))
}

// 解析出每个peer的hash值
func (torr *Torrent) getPieceHashs() [][20]byte {
	p_nums := torr.Info.Length / torr.Info.PieceLength
	piece_hashs := make([][20]byte, p_nums)
	piece_bts := []byte(torr.Info.Pieces)
	for i := 0; i < p_nums; i++ {
		copy(piece_hashs[i][:], piece_bts[i*20:(i+1)*20])
	}
	return piece_hashs
}

// 组建和tracker服务器交互的url
func (torr *Torrent) build_tracker_url(peer_id [20]byte, port int) string {
	base, _ := url.Parse(torr.Announce)
	info_hash := torr.getInfoHash()
	params := url.Values{
		"info_hash":  []string{string(info_hash[:])},
		"peer_id":    []string{string(peer_id[:])},
		"port":       []string{strconv.Itoa(Port)},
		"uploaded":   []string{"0"},
		"downloaded": []string{"0"},
		"compact":    []string{"1"},
		"left":       []string{strconv.Itoa(torr.Info.Length)},
	}
	base.RawQuery = params.Encode()
	return base.String()
}

// 通过 tracker 服务器获取 peers
// 各种获取peer的方法 (头大。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。)
func (torr *Torrent) getPeers(peerID [20]byte, port int) ([]peer, error) {
	resp, err := (&http.Client{Timeout: 15 * time.Second}).
		Get(torr.build_tracker_url(peerID, port))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// 临时结果数据结构
	type trackerResp struct {
		Interval int    `bc:"interval"`
		Peers    string `bc:"peers"`
	}

	// bencode 解码结果
	tResp := trackerResp{}
	bencode.NewBenCode("bc").Decode(string(content), &tResp)

	// 如果解析失败
	if tResp.Interval == 0 {
		type trackerFailureResp struct {
			FailureReason string `bc:"failure reason"`
		}
		failure := trackerFailureResp{}
		bencode.NewBenCode("bc").Decode(string(content), &failure)
		return nil, fmt.Errorf("tracker 服务器 [%s] 获取peer时返回错误 [%s]", torr.Announce, failure.FailureReason)
	}

	// 从二进制数据中解析出 peer 节点
	var parse_peers_from_bytes = func(bts []byte) ([]peer, error) {
		const peerSize = 6 // ip 4字节, port 2字节
		if len(bts)%peerSize != 0 {
			return nil, errors.New("解析出错")
		}

		numPeers := len(bts) / peerSize
		peers := make([]peer, numPeers)
		for i := 0; i < numPeers; i++ {
			offset := i * peerSize
			peers[i].IP = net.IP(bts[offset : offset+4])
			peers[i].Port = binary.BigEndian.Uint16([]byte(bts[offset+4 : offset+6]))
		}
		return peers, nil
	}

	return parse_peers_from_bytes([]byte(tResp.Peers))
}

// 解析出的 peer 端的数据结构
type peer struct {
	IP   net.IP
	Port uint16
}

func (p peer) String() string {
	return net.JoinHostPort(p.IP.String(), strconv.Itoa(int(p.Port)))
}

// 和 peers 的握手协议
type handshake struct {
	Pstr     string
	InfoHash [20]byte
	PeerID   [20]byte
}

func new_handshake(infoHash, peerID [20]byte) *handshake {
	return &handshake{Pstr: "BitTorrent protocol", InfoHash: infoHash, PeerID: peerID}
}

// 编码 握手协议
func (h *handshake) Serialize() []byte {
	buf := make([]byte, len(h.Pstr)+49)
	buf[0] = byte(len(h.Pstr))
	curr := 1
	curr += copy(buf[curr:], h.Pstr)
	curr += copy(buf[curr:], make([]byte, 8)) // 中间空8位(协议预留使用)
	curr += copy(buf[curr:], h.InfoHash[:])
	curr += copy(buf[curr:], h.PeerID[:])
	return buf
}

// 解码 握手协议
func read_handshake(r io.Reader) (*handshake, error) {
	lengthBuf := make([]byte, 1)
	_, err := io.ReadFull(r, lengthBuf)
	if err != nil {
		return nil, err
	}

	pstrlen := int(lengthBuf[0])

	handshakeBuf := make([]byte, 48+pstrlen)
	_, err = io.ReadFull(r, handshakeBuf)
	if err != nil {
		return nil, err
	}

	var infoHash, peerID [20]byte

	copy(infoHash[:], handshakeBuf[pstrlen+8:pstrlen+8+20])
	copy(peerID[:], handshakeBuf[pstrlen+8+20:])
	h := handshake{
		Pstr:     string(handshakeBuf[0:pstrlen]),
		InfoHash: infoHash,
		PeerID:   peerID,
	}
	return &h, nil
}

// 传输的消息类型
type MessageType uint8

// 枚举各种消息类型
const (
	// 阻塞类型
	MsgChoke MessageType = 0

	// 解除阻塞
	MsgUnchoke MessageType = 1

	// 有兴趣接受数据
	MsgInterested MessageType = 2

	// 对接受数据不感兴趣
	MsgNotInterested MessageType = 3

	// 提醒接受者已经下载了一个片段
	MsgHave MessageType = 4

	// *** 对发送者下载的片段进行编码 (此种类型消息会单独解析成一个结构体) ***
	// Peer用来有效编码他们能够发送给我们,告诉哪些文件片段的数据可以发送(下载完成)
	// 具体参考 bitfield 结构
	MsgBitfield MessageType = 5

	// 向接受者请求数据块
	MsgRequest MessageType = 6

	// 传递一个数据块, 完成请求
	// 真正传输数据块的消息类型
	MsgPiece MessageType = 7

	// 取消一个请求
	MsgCancel MessageType = 8
)

// 和peer传输的消息
type message struct {
	ID      MessageType // 消息类型
	Payload []byte      // 有效数据载体
}

// 消息序列化
// 消息序列化字节为: 长度1 长度2 长度3 长度4 消息类型 [有效数据载体 ...]
func (m *message) Serialize() []byte {
	if m == nil {
		return make([]byte, 4)
	}
	length := uint32(len(m.Payload) + 1) // +1 for id
	buf := make([]byte, 4+length)
	binary.BigEndian.PutUint32(buf[0:4], length)
	buf[4] = byte(m.ID)
	copy(buf[5:], m.Payload)
	return buf
}

func (m *message) name() string {
	if m == nil {
		return "KeepAlive"
	}

	switch m.ID {
	case MsgChoke:
		return "Choke"
	case MsgUnchoke:
		return "Unchoke"
	case MsgInterested:
		return "Interested"
	case MsgNotInterested:
		return "NotInterested"
	case MsgHave:
		return "Have"
	case MsgBitfield:
		return "Bitfield"
	case MsgRequest:
		return "Request"
	case MsgPiece:
		return "Piece"
	case MsgCancel:
		return "Cancel"
	default:
		return fmt.Sprintf("Unknown#%d", m.ID)
	}
}

func (m *message) String() string {
	if m == nil {
		return m.name()
	}
	return fmt.Sprintf("%s [%d]", m.name(), len(m.Payload))
}

// 创建一个require类型的message
func new_require_message(index, begin, length int) *message {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))
	return &message{ID: MsgRequest, Payload: payload}
}

// 创建一个have类型的message
func new_have_message(index int) *message {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(index))
	return &message{ID: MsgHave, Payload: payload}
}

// 解析 数据块消息
func parse_piece_message(index int, buf []byte, msg *message) (int, error) {
	// 校对数据类型
	if msg.ID != MsgPiece {
		return 0, fmt.Errorf("错误的消息类型：期望 PIECE (%d), 实际(%d)", MsgPiece, msg.ID)
	}

	// 校对数据量
	if len(msg.Payload) < 8 {
		return 0, fmt.Errorf("数据量太小. [%d] < 8", len(msg.Payload))
	}

	// 校对数据编号
	i := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
	if i != index {
		return 0, fmt.Errorf("期望的编号 %d, 实际 %d", index, i)
	}

	// 实际数据开始处
	begin := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
	if begin >= len(buf) {
		return 0, fmt.Errorf("读取数据头失败, 数据头(%d) >= 总数据量(%d)", begin, len(buf))
	}

	// 校验读取数据是否正确
	data := msg.Payload[8:]
	if begin+len(data) > len(buf) {
		return 0, fmt.Errorf("校验数据失败, 数据头(%d) + 数据长度(%d) > 数据(%d)", len(data), begin, len(buf))
	}
	copy(buf[begin:], data)
	return len(data), nil
}

func parse_have_message(msg *message) (int, error) {
	if msg.ID != MsgHave {
		return 0, fmt.Errorf("错误的消息类型：期望 HAVE (%d), 实际(%d)", MsgHave, msg.ID)
	}
	if len(msg.Payload) != 4 {
		return 0, fmt.Errorf("其他的数据量为 4, 实际为 %d", len(msg.Payload))
	}
	index := int(binary.BigEndian.Uint32(msg.Payload))
	return index, nil
}

// 解析消息
// 消息序列化字节为: 长度1 长度2 长度3 长度4 消息类型 [有效数据载体 ...]
func read_message(r io.Reader) (*message, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(lengthBuf)
	if length == 0 {
		return nil, nil
	}

	messageBuf := make([]byte, length)
	if _, err := io.ReadFull(r, messageBuf); err != nil {
		return nil, err
	}

	return &message{
		ID:      MessageType(messageBuf[0]),
		Payload: messageBuf[1:],
	}, nil
}

// 如果message类型为 MsgBitfield ， 从数据中解析出来的结构
type bitfield []byte

// 这个数据块是否已经有了
func (bf bitfield) hasPiece(index int) bool {
	byteIndex := index / 8
	offset := index % 8
	return bf[byteIndex]>>(7-offset)&1 != 0
}

func (bf bitfield) set_piece(index int) {
	byteIndex := index / 8
	offset := index % 8
	bf[byteIndex] |= 1 << (7 - offset)
}

type client struct {
	Conn      net.Conn
	Choked    bool
	Bitfield  bitfield
	p         peer
	info_hash [20]byte
	peer_id   [20]byte
}

// 进行握手
func complete_handshake(conn net.Conn, infohash, peerID [20]byte) (*handshake, error) {
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	defer conn.SetDeadline(time.Time{}) // Disable the deadline

	req := new_handshake(infohash, peerID)
	_, err := conn.Write(req.Serialize())
	if err != nil {
		return nil, err
	}

	res, err := read_handshake(conn)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(res.InfoHash[:], infohash[:]) {
		return nil, fmt.Errorf("期望的 infohash %x 但是得到 %x", res.InfoHash, infohash)
	}
	return res, nil
}

// 接受 bitfield
func recvBitfield(conn net.Conn) (bitfield, error) {
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetDeadline(time.Time{})

	msg, err := read_message(conn)

	if err != nil {
		return nil, err
	}

	if msg == nil {
		err := fmt.Errorf("消息为 ", msg)
		return nil, err
	}

	if msg.ID != MsgBitfield {
		err := fmt.Errorf("期望 bitfield 类型消息，但是得到的类型 ID 为 %d", msg.ID)
		return nil, err
	}

	return msg.Payload, nil
}

// 新建一个和peer的连接，并完成握手
func new_client(p peer, peer_id, info_hash [20]byte) (*client, error) {
	conn, err := net.DialTimeout("tcp", p.String(), 3*time.Second)
	if err != nil {
		return nil, err
	}

	// 完成握手
	_, err = complete_handshake(conn, info_hash, peer_id)
	if err != nil {
		conn.Close()
		return nil, err
	}

	// 接受 Bitfield 类型消息
	bf, err := recvBitfield(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &client{
		Conn:      conn,
		Choked:    true,
		Bitfield:  bf,
		p:         p,
		info_hash: info_hash,
		peer_id:   peer_id,
	}, nil
}

// 读取一条消息
func (c *client) read() (*message, error) {
	msg, err := read_message(c.Conn)
	return msg, err
}

// 向 peer 发送一条 require 消息
func (c *client) sendRequest(index, begin, length int) error {
	req := new_require_message(index, begin, length)
	_, err := c.Conn.Write(req.Serialize())
	return err
}

// 向 peer 发送一条 Interested 消息
func (c *client) sendInterested() error {
	msg := message{ID: MsgInterested}
	_, err := c.Conn.Write(msg.Serialize())
	return err
}

// 向 peer 发送一条 NotInterested 消息
func (c *client) sendNotInterested() error {
	msg := message{ID: MsgNotInterested}
	_, err := c.Conn.Write(msg.Serialize())
	return err
}

// 向 peer 发送一条 Unchoke 消息
func (c *client) sendUnchoke() error {
	msg := message{ID: MsgUnchoke}
	_, err := c.Conn.Write(msg.Serialize())
	return err
}

// 向 peer 发送一条 Have 消息
func (c *client) sendHave(index int) error {
	msg := new_have_message(index)
	_, err := c.Conn.Write(msg.Serialize())
	return err
}
