#!/usr/bin/env perl 
#===============================================================================
#
#         FILE: main.pl
#
#        USAGE: ./main.pl  
#
#  DESCRIPTION: 
#
#      OPTIONS: ---
# REQUIREMENTS: Config::Tiny, AnyEvent, AnyEvent::Loop, AnyEvent::Handle::UDP, DBI, DBD::mysql
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: Ilya Suhov (), tovarischsuhov0@yandex.ru
# ORGANIZATION: 
#      VERSION: 1.0
#      CREATED: 10.05.2014 17:58:41
#     REVISION: ---
#===============================================================================

use strict;
use warnings;
use utf8;
use Config::Tiny;
use AnyEvent;
use AnyEvent::Loop;
use AnyEvent::Handle::UDP;
use DBI;
use DBD::mysql;

sub onrecieve;
sub dbcheck;
sub check_slave_status;
sub get_address;
sub get_ipv4;
sub get_ipv6;
sub switch_to;
sub switchover;
sub votecount;
sub onus1recv;

### Reading config

my $config = Config::Tiny->new;
$config = Config::Tiny->read( 'mysql_repl.conf' );

my %selfstatus;
my %sql;
my $master;
my $tmp1;
my $tmp2;

$selfstatus{id} = $config->{settings}->{id};
$selfstatus{hostname} = $config->{settings}->{hostname};
$selfstatus{protocol} = $config->{settings}->{protocol};
$selfstatus{maxdelay} = $config->{settings}->{maxdelay};
$selfstatus{port} = $config->{settings}->{port};

$selfstatus{port} = 4000 if not defined $selfstatus{port};
$selfstatus{protocol} = "IPv4" if not defined $selfstatus{protocol};
$selfstatus{maxdelay} = 20 if not defined $selfstatus{maxdelay};

$sql{user} = $config->{sql}->{user};
$sql{passwd} = $config->{sql}->{passwd};
$sql{root} = $config->{sql}->{root};
$sql{rootpasswd} = $config->{sql}->{rootpasswd};

$selfstatus{address} = get_address($selfstatus{hostname});

my %hosts;
for my $x(keys %{$config}){
	if($x =~ /host\d+/){
	$hosts{$config->{$x}->{id}}->{address} = get_address($config->{$x}->{address});
	$hosts{$config->{$x}->{id}}->{host} = $config->{$x}->{address};
	$hosts{$config->{$x}->{id}}->{status} = "PING_OK";
	}
}

$hosts{$selfstatus{id}}->{address} = $selfstatus{address};
$hosts{$selfstatus{id}}->{host} = $selfstatus{hostname};


my $udp = AnyEvent::Handle::UDP->new(
	bind => ["0.0.0.0", $selfstatus{port}],
	on_recv => \&onrecieve,
);

my $termsig= AnyEvent->signal (signal => "TERM", cb => sub {exit 0});
my $switchsig = AnyEvent->signal (signal => "USR1", cb => \&onusr1recv);
my $dbcheck = AnyEvent->timer(interval => 5, cb => \&dbcheck);
my $check_slaves = AnyEvent->timer(interval => 5, cb => \&check_slave_status);

###Here would be initialization part

my $dbh = DBI->connect("DBI:mysql:mysql",$sql{root},$sql{rootpasswd});
my $sth = $dbh->prepare("SHOW SLAVE STATUS");
$sth->execute;
my $ref = $sth->fetchrow_hashref;

if((not defined $ref) or ($ref->{Master_Host} eq $selfstatus{hostname})){
	$selfstatus{role} = "master";
} 
else{
	$selfstatus{role} = "slave";
}
$sth->finish;
$dbh->disconnect;

for my $x (keys %hosts){
	$udp->push_send(qq($selfstatus{id}:ASK_INFO;),$hosts{$x}->{address});
	$udp->push_send(qq($selfstatus{id}:INFO;$selfstatus{role}),$hosts{$x}->{address})
}
#
sub check_db{
	my $dbh = DBI->connect("DBI:mysql:mysql",
							$sql{root}, $sql{rootpasswd},{RaiseError => 0});
	return 0 if not defined $dbh;
	eval{$dbh->do("SELECT * FROM user")};
	return 0 if $@;
	$dbh->disconnect;
	return 1;
}

sub check_slave_status{
	my $time = AnyEvent->now;
	for my $x(keys %hosts){
		if($hosts{$x}->{status} eq "DB_DOWN"){
			warn "Host #$x is droped";
		}
		elsif($time - $hosts{$x}->{time} > $selfstatus{maxdelay}){
		$hosts{$x}->{status} = "DB_DOWN";
		warn "Host #$x is droped now";
		}
		else{warn "Host #$x is OK"};
	}
}

sub dbcheck{
	my $time = AnyEvent->now;
	if(check_db){ # check db status
		for my $x(keys %hosts){
#			warn qq(Send PING_OK to $x, $hosts{$x}->{address}->[0], $hosts{$x}->{address}->[1]);
			$udp->push_send(qq($selfstatus{id}:PING_OK;$time),$hosts{$x}->{address});
		}
	}
	else{
		for my $x(keys %hosts){
#			warn qq(Send DB_DOWN to $x, $hosts{$x}->{address}->[0], $hosts{$x}->{address}->[1]);
			$udp->push_send(qq($selfstatus{id}:DB_DOWN;$time),$hosts{$x}->{address});
		}	
	}
}

sub onrecieve{
	my ($data, $handle, $addr) = @_;
	$data =~ /(\d+):([_A-Z]+);(.*)?/;
	my($id ,$status, $args) = ($1, $2, $3);
	warn "Recieved $data";
	if($status eq "PING_OK"){ # status ok
		$hosts{$id}->{status} = $status;
		$hosts{$id}->{time} = $args;
#		warn qq($id $hosts{$id}->{status} $hosts{$id}->{time}); # debuging info
	}
	elsif($status eq "DB_DOWN"){ # db is down
		$hosts{$id}->{status} = $status;
#		warn qq($id $hosts{$id}->{status} $hosts{$id}->{time}); # debuging info
	}
	elsif($status eq "SWITCH_TO"){ # command switch
		my($id, $file, $position) = split /;/,$args;
		switch_to($hosts{$id}->{host}, $file,$position);
		$hosts{$master}->{role} = "slave";
		$master = $id;
		$hosts{$id}->{role} = "master";
		$selfstatus{role} = $hosts{$selfstatus{id}}->{role};
	}
	elsif($status eq "LOAD_FROM"){ # command to load
			
	}
	elsif($status eq "SWITCH_OVER"){ # command to master to switch_over
		my $dbh = DBI->connect("DBI:mysql:mysql", $sql{root}, $sql{rootpasswd});
		$dbh->do("FLUSH TABLES WITH READ LOCK");
		$dbh->disconnect;
		for my $x(keys %hosts){
			$udp->push_send(qq($selfstatus{id}:START_VOTE;), $hosts{$x}->{address});
		}
		warn "Starts vote count";
		$tmp1 = AnyEvent->timer(after => 5, cb => \&votecount)
		
	}
	elsif($status eq "ASK_INFO"){
		$udp->push_send(qq($selfstatus{id}:INFO;$selfstatus{role}), $hosts{$id}->{address});
	}
	elsif($status eq "INFO"){
		$hosts{$id}->{role} = $args;
		if($args eq "master"){
			$master = $id;
		}
	}
	elsif($status eq "VOTE"){
		my($vote, $time) = split /;/, $args;
		$hosts{$id}->{vote}->{id} = $vote;
		$hosts{$id}->{vote}->{time} = $time;
	}
	elsif($status eq "START_VOTE"){
		my @tmp;
		my $time = AnyEvent->now;
		for my $x(keys %hosts){
			if($hosts{$x}->{status} eq "PING_OK" && $x ne $master){
				push @tmp, $x;
			}
		}
		my $select = int(rand(scalar @tmp));
		$udp->push_send(qq($selfstatus{id}:VOTE;$tmp[$select];$time), $hosts{$id}->{address});
	}
	elsif($status eq "NEW_MASTER"){
		warn "I would be new master";
		my $waittime = 2;
		my $dbh = DBI->connect("DBI:mysql:mysql", $sql{root}, $sql{rootpasswd});
		my $sth = $dbh->prepare("SHOW SLAVE STATUS");
		$sth->execute;
		my $ref = $sth->fetchrow_hashref;
		$waittime += $ref->{Seconds_Behind_Master};
		$sth->finish;
		$dbh->disconnect;
		$tmp2 = AnyEvent->timer(after => $waittime, cb => \&switchover);
	}
}

sub get_address{
	my $address;
	my $hostname = shift;
	if($selfstatus{protocol} eq "IPv6"){
			my $tmp = get_ipv6($hostname);
			if (!defined $tmp){
				$selfstatus{protocol} = "IPv4";
				$address = get_address($hostname);
			}
			return [$tmp,$selfstatus{port}]
	}
	elsif($selfstatus{protocol} eq "IPv4"){
		   return [get_ipv4($hostname), $selfstatus{port}];
	}
	else{
		warn qq(Unknown protocol $selfstatus{protocol})
	}
	return $address;
}

sub get_ipv4{
	my $hostname = shift;
 	if($hostname =~ /^((?:\d{1,3}\.){3}\d{1,3})$/){return $hostname}
	my $hosts = `host $hostname`;
	$hosts =~ /((?:\d{1,3}\.){3}\d{1,3})/m;
	return $1;
}

sub get_ipv6{
	my $hostname = shift;
	if($hostname =~ /^([0-9a-f:]{4,})$/){return $hostname}
	my $hosts = `host $hostname`;
	$hosts =~ /([0-9a-f:]{4,})/m;
	return $1;
}

sub switch_to{
	my $hostname = shift;
	my $filename = shift;
	warn "Starts switch to host #$hostname";
	my $position = shift;
	my $query = qq(change master to master_host="$hostname", master_user="$sql{user}", master_password="$sql{passwd}", master_log_file="$filename", master_log_pos=$position);
	my $dbh = DBI->connect("DBI:mysql:mysql", $sql{root}, $sql{rootpasswd});
	$dbh->do("STOP SLAVE");
	$dbh->do($query);
	$dbh->do("START SLAVE") if $hostname ne $selfstatus{hostname};
	$dbh->do("UNLOCK TABLES");
	$dbh->disconnect;
	warn "Already switched to host #$hostname";
}

sub votecount{
	warn "Vote starts";
	my @tmp;
	my $max = 0;
	my $maxvote;
	my $time = AnyEvent->now;
	for my $x(keys %hosts){
		warn "$time:$hosts{$x}->{vote}->{time}:$hosts{$x}->{status}:$hosts{$x}->{vote}->{id}:$x";
		if($time - $hosts{$x}->{vote}->{time} < $selfstatus{maxdelay} && $hosts{$x}->{status} eq "PING_OK"){
			$tmp[$hosts{$x}->{vote}->{id}]++;
		}
	}
	for(my $i; $i < scalar(@tmp);$i++){
		warn "blablabla";
		if($tmp[$i] > $max){
			$max = $tmp[$i];
			$maxvote = $i;
		}
	}
	warn "New master will be #$maxvote";
	$udp->push_send(qq($selfstatus{id}:NEW_MASTER;), $hosts{$maxvote}->{address});
}

sub switchover{
	warn "Starting switchover";
	my $dbh = DBI->connect("DBI:mysql:mysql", $sql{root}, $sql{rootpasswd});
	my $sth = $dbh->prepare("SHOW MASTER STATUS");
	$sth->execute;
	my $ref = $sth->fetchrow_hashref;
	$sth->finish;
	$dbh->disconnect;
	my $file = $ref->{File};
	my $pos = $ref->{Position};
	for my $x(keys %hosts){
		$udp->push_send(qq($selfstatus{id}:SWITCH_TO;$selfstatus{id};$file;$pos),$hosts{$x}->{address});
	}
}

sub onusr1recv{
	warn "Get USR1 signal";
	$udp->push_send(qq($selfstatus{id}:SWITCH_OVER;),$hosts{$master}->{address});
	warn "SWITCH_OVER message is sent";
}


AnyEvent::Loop::run; # main loop


__END__

=head3 STATUSES

	ID:PING_OK;[time] - regulary comes, sends creation times

	ID:DB_DOWN;[time] - shows when DB on sender is down 

	ID:SWITCH_TO;[id;file;pos] - commands to switch master to host

	ID:LOAD_FROM;[id] - commands to load DB dump from host

	ID:SWITCH_OVER; - commands master to switch master to host

	ID:ASK_INFO; - asks about roles

	ID:INFO;[role] - tells role (master, slave)

	ID:VOTE;[id;time] - votes for host with id

	ID:START_VOTE; - asks for vote

	ID:NEW_MASTER; - tels host that it is new master
