import rpyc
import sys
import os
import random
import time
import threading
import logging

logging.basicConfig(filename='debug.log', level=logging.DEBUG)

'''
A RAFT RPC server class.

Please keep the signature of the is_leader() method unchanged (though
implement the body of that function correctly.  You will need to add
other methods to implement ONLY the leader election part of the RAFT
protocol.
'''


class RaftNode(rpyc.Service):
    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
    """

    def __init__(self, config, server_idx, server_port):
        self.server_idx = server_idx
        self.server_port = server_port
        self.config = config
        self.num_node, self.host_field, self.port_field = self.parse_config()
        self.file_name = '/tmp/state_info' + str(self.server_idx) + '.txt'
        self.state = 'follower'
        self.current_term, self.vote_for = self.read_from_disk()
        # self.leader_id = -1
        self.request_vote_lock = threading.Lock()
        self.append_entries_lock = threading.Lock()
        self.write_lock = threading.Lock()
        self.timer = threading.Timer(random.uniform(1, 5), self.start_election)
        self.timer.start()
        self.processed = 0
        self.votes = 0

    '''
        x = is_leader(): returns True or False, depending on whether
        this node is a leader

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call

        CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
    '''

    def read_from_disk(self):
        if os.path.isfile(self.file_name):
            state_file = open(self.file_name, 'r')
            current_term = int(state_file.readline())
            vote_for = int(state_file.readline())
            state_file.close()
        else:
            current_term = 0
            vote_for = None
        return current_term, vote_for

    def write_to_disk(self, term, vote_for):
        with self.write_lock:
            state_file = open(self.file_name, 'w')
            state_file.write(str(term) + '\n' + str(vote_for))
            state_file.flush()
            os.fsync(state_file.fileno())
            state_file.close()

    def exposed_is_leader(self):
        return self.state == 'leader'

    def exposed_append_entries(self, term, leader_id):
        with self.append_entries_lock:
            self.timer.cancel()
            success = True
            if term < self.current_term:
                success = False
            if success:
                if term > self.current_term:
                    self.vote_for = -1
                self.current_term = term
                # self.leader_id = leader_id
                self.state = 'follower'
                print(self.state)
                t = threading.Thread(target=self.write_to_disk, args=(self.current_term, self.vote_for))
                t.start()
                # self.write_to_disk(self.current_term, self.vote_for)
            print('%d receive append entries from %d, send %s' % (self.server_idx, leader_id, success))
            logging.info('%d receive append entries from %d, send %s' % (self.server_idx, leader_id, success))
            self.timer = threading.Timer(random.uniform(1, 5), self.start_election)
            self.timer.start()
            return self.current_term, success

    def exposed_request_vote(self, term, candidate_id):
        with self.request_vote_lock:
            if term < self.current_term:
                return self.current_term, False, self.host_field[self.server_idx], self.server_port
            elif term > self.current_term:
                self.vote_for = candidate_id
                self.current_term = term
                t = threading.Thread(target=self.write_to_disk, args=(self.current_term, self.vote_for))
                t.start()
                # self.write_to_disk(self.current_term, self.vote_for)
                return self.current_term, True, self.host_field[self.server_idx], self.server_port
            else:
                if not self.vote_for or self.vote_for == -1 or self.vote_for == candidate_id:
                    self.vote_for = candidate_id
                    t = threading.Thread(target=self.write_to_disk, args=(self.current_term, self.vote_for))
                    t.start()
                    # self.write_to_disk(self.current_term, self.vote_for)
                    return self.current_term, True, self.host_field[self.server_idx], self.server_port
                else:
                    return self.current_term, False, self.host_field[self.server_idx], self.server_port

    def heat_beat_value(self, node_ip, node_port, heart_beat_echo):
        lock = threading.Lock()
        try:
            conn = rpyc.connect(node_ip, node_port)
            async_call = rpyc.timed(conn.root.append_entries, 0.5)
            result = async_call(self.current_term, self.server_idx)
            heart_beat_echo.append(result)
            term, success = result.value
            if term > self.current_term:
                lock.acquire()
                self.state = 'follower'
                self.vote_for = -1
                self.current_term = term
                self.write_to_disk(self.current_term, self.vote_for)
                lock.release()
        except Exception:
            pass
        finally:
            self.processed += 1
        return


    def run_as_leader(self):
        self.timer.cancel()
        # while self.state == 'leader' and self.leader_id == self.server_idx:
        while True:
            heart_beat_echo = []
            print(self.state)
            self.processed = 0
            sent = 0
            for i in range(len(self.host_field)):
                if self.state != 'leader':
                    return
                node_ip = self.host_field[i]
                node_port = self.port_field[i]
                if node_ip == self.host_field[self.server_idx] and node_port == self.server_port:
                    continue
                # try:
                    # conn = rpyc.connect(node_ip, node_port)
                t = threading.Thread(target=self.heat_beat_value, args=(node_ip, node_port, heart_beat_echo))
                t.start()
                sent += 1
                    # async_call = rpyc.timed(rpyc.connect(node_ip, node_port).root.append_entries, 0.2)
                    # heart_beat_echo.append(async_call(self.current_term, self.server_idx))
                # except Exception:
                #     pass
            while self.processed < sent:
                if self.state == 'follower':
                    self.timer = threading.Timer(random.uniform(1, 5), self.start_election)
                    self.timer.start()
                    return

            # for echo in heart_beat_echo:
            #
            #     try:
            #         term, success = echo.value
            #         if not success:
            #             self.current_term = term
            #             self.state = 'follower'
            #             print(self.state)
            #             # self.leader_id = -1
            #             self.vote_for = -1
            #             t = threading.Thread(target=self.write_to_disk, args=(self.current_term, self.vote_for))
            #             t.start()
            #             # self.write_to_disk(self.current_term, self.vote_for)
            #             self.timer = threading.Timer(random.uniform(2, 5), self.start_election)
            #             self.timer.start()
            #             return
            #     except Exception:
            #         pass
            # send heartbeats every 50ms
            time.sleep(0.1)

    def request_vote_value(self, node_ip, node_port, request_vote_echo, voted_servers):
        lock = threading.Lock()
        try:
            conn = rpyc.connect(node_ip, node_port)
            async_call = rpyc.timed(conn.root.request_vote, 0.2)
            result = async_call(self.current_term, self.server_idx)
            request_vote_echo.append(result)
            term, vote_granted, server_ip, server_port = result.value
            voted_servers.append((server_ip, server_port))
            print(term, vote_granted)
            if vote_granted:
                self.votes += 1
                print(self.votes)
            else:
                if term > self.current_term:
                    lock.acquire()
                    self.current_term = term
                    self.state = 'follower'
                    self.vote_for = -1
                    self.write_to_disk(self.current_term, self.vote_for)
                    lock.release()
        except Exception as e:
            print(e)
        finally:
            self.processed += 1
        return

    def set_timeout(self):
        self.timeout = True
        self.start_election()

    def start_election(self):
        self.timeout = False
        self.state = 'candidate'
        print(self.state)
        self.votes = 1
        voted_servers = []
        self.current_term += 1
        self.vote_for = self.server_idx
        t = threading.Thread(target=self.write_to_disk, args=(self.current_term, self.vote_for))
        t.start()
        log_term = self.current_term
        self.timer = threading.Timer(random.uniform(1, 5), self.start_election)
        self.timer.start()
        while self.state == 'candidate' and log_term == self.current_term:
            request_vote_echo = []
            self.processed = 0
            sent = 0
            for i in range(len(self.host_field)):
                node_ip = self.host_field[i]
                node_port = self.port_field[i]
                if (node_ip == self.host_field[self.server_idx] and node_port == self.server_port)\
                        or (node_ip, node_port) in voted_servers:
                    continue
                try:
                    # conn = rpyc.connect(node_ip, node_port)
                    t = threading.Thread(target=self.request_vote_value, args=(node_ip, node_port, request_vote_echo,
                                                                               voted_servers))
                    t.start()
                    sent += 1
                    # async_call = rpyc.timed(rpyc.connect(node_ip, node_port).root.request_vote, 0.2)
                    # request_vote_echo.append(async_call(self.current_term, self.server_idx))
                except Exception:
                    pass

            while self.processed < sent:
                if self.state == 'follower':
                    self.timer = threading.Timer(random.uniform(1, 5), self.start_election)
                    self.timer.start()
                    return
                # print('tongji', self.votes)
                if self.votes >= (self.num_node // 2 + 1):
                    self.state = 'leader'
                    print('leader elected!!!!!!')
                    self.timer.cancel()
                    t = threading.Thread(target=self.run_as_leader)
                    t.start()
                    return

            # for echo in request_vote_echo:
            #     try:
            #         term, vote_granted, server_ip, server_port = echo.value
            #         if term > self.current_term:
            #             self.current_term = term
            #             self.state = 'follower'
            #             self.vote_for = -1
            #             t = threading.Thread(target=self.write_to_disk, args=(self.current_term, self.vote_for))
            #             t.start()
            #             # self.write_to_disk(self.current_term, self.vote_for)
            #             self.timer.cancel()
            #             print(self.state)
            #             self.timer = threading.Timer(random.uniform(3, 6), self.start_election)
            #             self.timer.start()
            #             return
            #         if vote_granted:
            #             voted_servers.append((server_ip, server_port))
            #             self.votes += 1
            #         if self.votes >= (self.num_node // 2 + 1):
            #             self.state = 'leader'
            #             print('leader elected!!!!!!')
            #             # self.leader_id = self.server_idx
            #             self.timer.cancel()
            #             self.run_as_leader()
            #             return
            #     except Exception:
            #         pass
            time.sleep(0.25)
        return

    def parse_config(self):
        config = self.config
        config_file = open(config, 'r')
        first_line = config_file.readline()
        idx = first_line.find(': ')
        num_node = int(first_line[idx + 2:])
        next_line = config_file.readline()
        host_field = []
        port_field = []
        while next_line:
            this_line = next_line
            idx = this_line.find(': ')
            idx2 = this_line.find(':', idx + 2)
            host_field.append(this_line[idx + 2:idx2])
            idx_next_line = this_line.find('\r\n', idx2)
            if idx_next_line != -1:
                port_field.append(int(this_line[idx2 + 1:idx_next_line]))
            else:
                port_field.append(int(this_line[idx2 + 1:]))
            next_line = config_file.readline()
        config_file.close()
        return num_node, host_field, port_field


if __name__ == '__main__':
    from rpyc.utils.server import ThreadedServer

    server = ThreadedServer(RaftNode(sys.argv[1], int(sys.argv[2]), int(sys.argv[3])), port=int(sys.argv[3]))
    server.start()