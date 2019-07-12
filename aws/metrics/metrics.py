import urllib.request, json
import time as t
import threading
import numpy as np
import scipy.stats as st


def get_current_metric(metric_name, job_id, vertex_id, hostname, port, l):
	if l == 0:
		request = "http://" + hostname + ":" + port + "/jobs/" + job_id + "/vertices/" + vertex_id + "/metrics?get=" + metric_name
		print(request)
		response = urllib.request.urlopen(request).read()
		metric = json.loads(response)[0]["value"]
		return t.time(), metric
	elif l == 1 :
		request = "http://" + hostname + ":" + port + "/taskmanagers/" + job_id + "/metrics?get=" + metric_name
		print(request)
		response = urllib.request.urlopen(request).read()
		metric = json.loads(response)[0]["value"]
		return t.time(), metric
	elif l == 2:
		request = "http://" + hostname + ":" + port + "/jobs/" + job_id + "/metrics?get=" + metric_name
		print(request)
		response = urllib.request.urlopen(request).read()
		metric = json.loads(response)[0]["value"]
		return t.time(), metric
	elif l == 3: #latency
		request1 = "http://" + hostname + ":" + port + "/jolokia/read/kafka.streams:" + job_id
		request2 = "http://" + hostname + ":" + port + "/jolokia/read/kafka.streams:" + vertex_id

		print(request1)
		print(request2)

		response1 = urllib.request.urlopen(request1).read()
		metric_1 = json.loads(response1)['value'][metric_name]

		if metric_1 is None:
			metric_1 = 0.0

		response2 = urllib.request.urlopen(request2).read()
		metric_2 = json.loads(response2)['value'][metric_name]
		if metric_2 is None:
			metric_2 = 0.0

		return t.time(), (metric_1 + metric_2)
	elif l == 4: # throughput
		request = "http://" + hostname + ":" + port + "/jolokia/read/kafka.streams:" + job_id
		print(request)
		response = urllib.request.urlopen(request).read()
		metric = json.loads(response)['value'][vertex_id]

		return t.time(), metric
	elif l == 5:
		request = "http://" + hostname + ":" + port
		print(request)
		response = urllib.request.urlopen(request).read()
		if response is not None:
			metric = float(response)
		else:
			metric = 0.0

		return t.time(), metric




def monitor_metric(duration, time_interval, metric_name, job_id, vertex_id, hostname, yaxis, multiplier=1, port="8081", l=0):
	f = open(metric_name + ".txt", "w")
	f1 = open(metric_name + "raw.txt", "w")
	initial_time = t.time()
	time_stamps = []
	metric_values = []
	while (t.time() - initial_time < duration):
		time, metric = get_current_metric(metric_name, job_id, vertex_id, hostname, port, l)
		print(str(time) + "," + str(metric) + "\n", file=f1)
		time_stamps.append(float(time - initial_time))
		metric_values.append(float(metric) * multiplier)
		t.sleep(time_interval)
	f1.close()
	mean = np.mean(metric_values)
	conf_interval = confidence_interval(metric_values, 0.95)
	perc99 = np.percentile(metric_values, 99)
	per999 = np.percentile(metric_values, 99.9)
	print("mean=" + str(mean) + "\nconf_interval=" + str(conf_interval) + "\nperc99=" + str(perc99) + "\nperc999=" + str(
		per999) + "\nn=" + str(len(metric_values)), file=f)
	f.close()
	mutex.acquire()

	import matplotlib.pyplot as plt
	plt.plot(time_stamps, metric_values)
	plt.xlabel("time [s]")
	plt.ylabel(yaxis)
	plt.savefig(metric_name + ".png")
	plt.close()

	mutex.release()


def confidence_interval(data, confidence=0.95):
	a = 1.0 * np.array(data)
	n = len(a)
	se = st.sem(a)
	h = se * st.t.ppf((1 + confidence) / 2., n - 1)
	return h




def spawn_thread(duration, time_interval, metric_name, job_id, vertex_id, hostname, yaxis, multiplier=1, port="8088", l=0):

		#p = Process(target=monitor_metric, args=(duration, time_interval, metric_name, job_id, vertex_id, hostname, yaxis, multiplier, port, l,))
		#p.start()
		threading.Thread(target=monitor_metric, args=(
		duration, time_interval, metric_name, job_id, vertex_id, hostname, yaxis, multiplier, port, l)).start()




def main():
	minutes = 10
	job_id = "client-id=SABD-project-2-61855e2f-2b9d-4033-85a1-bdd67ec4b330-StreamThread-1,processor-node-id=KSTREAM-FILTER-0000000006,task-id=0_0,type=stream-processor-node-metrics"
	vertex_id = 'client-id=SABD-project-2-61855e2f-2b9d-4033-85a1-bdd67ec4b330-StreamThread-1,processor-node-id=KSTREAM-MAP-0000000002,task-id=0_0,type=stream-processor-node-metrics'
	taskmanager = 'container_1562523021289_0003_01_000009'

	spawn_thread(minutes * 60, 1, "process-rate-in",
				 "client-id=SABD-project-22222-4d7bc30f-966d-4731-94f5-b3596c83b487-StreamThread-1,processor-node-id=KSTREAM-SOURCE-0000000008,task-id=1_0,type=stream-processor-node-metrics",
				 "process-rate",
				 "localhost",
				 "throughput [event/s]",
				 1,
				 port="8779",
				 l=4)

	#spawn_thread(minutes * 60, 1, "process-rate-pre-window",
	#			 "client-id=SABD-project-222-76a36aa6-dceb-429e-88d7-bc6d7e51aaa3-StreamThread-1,processor-node-id=KSTREAM-FILTER-0000000022,task-id=0_0,type=stream-processor-node-metrics",
	#			 "process-rate",
	#			 "localhost",
	#			 "throughput [event/s]",
	#			 1,
	#			 port="8779",
	#			 l=4)

	spawn_thread(minutes * 60, 1, "utilization", "",
				 "", "localhost",
				 "perc [%]", 1, port="9999", l=5)

	spawn_thread(minutes * 60, 1, "process-latency-avg",  "client-id=SABD-project-22222-4d7bc30f-966d-4731-94f5-b3596c83b487-StreamThread-1,processor-node-id=KSTREAM-SOURCE-0000000008,task-id=1_0,type=stream-processor-node-metrics",
				 "client-id=SABD-project-22222-4d7bc30f-966d-4731-94f5-b3596c83b487-StreamThread-1,processor-node-id=KSTREAM-REDUCE-0000000005,task-id=1_0,type=stream-processor-node-metrics", "localhost",
				 "latency [ms]", 10**(-6), port="8779", l=3)










	#spawn_thread(minutes * 60, 1, "0.Process.Query3.histogram_mean", job_id,
	#			 vertex_id, "localhost",
	#			 "latency [ms]", 10**(-6))
	#spawn_thread(minutes * 60, 1, "0.Process.Query3.throughput_in", job_id,
	#			 vertex_id, "localhost",
	#			 "throughput [tuple/s]", 1)
	#spawn_thread(minutes * 60, 1, "0.Process.Query3.thoughput_pre_window", job_id,
	#			 vertex_id, "localhost",
	#			 "throughput [tuple/s]", 1)
	#
	#spawn_thread(minutes * 60, 1, 'System.CPU.Usage', taskmanager,
	#			 "", "localhost",
	#			 "percentage [%]", 1, l=1)
	#
	#spawn_thread(minutes*60, 1, "latency.source_id.e3dfc0d7e9ecd8a43f85f0b68ebf3b80.operator_id.737315ee1d1f309dbf6c6e6a063db1c6.operator_subtask_index.0.latency_mean",
    #             job_id, "", "localhost", "latency [s]", 60, l=2)


	#spawn_thread(10 * 60, 1, "1.Process.Query1.histogram_mean", "4875ce43bed35b7faaa5825947b43b24",
	#			 "cbc357ccb763df2852fee8c4fc7d55f2", "localhost",
	#			 "latency [ns]", 10 ** (-6))
	#spawn_thread(10 * 60, 1, "1.Process.Query1.throughput_in", "4875ce43bed35b7faaa5825947b43b24",
	#			 "cbc357ccb763df2852fee8c4fc7d55f2", "localhost",
	#			 "latency [ns]", 10 ** (-6))
	#spawn_thread(10 * 60, 1, "1.Process.Query1.thoughput_pre_window", "4875ce43bed35b7faaa5825947b43b24",
	#			 "cbc357ccb763df2852fee8c4fc7d55f2", "localhost",
	#			 "latency [ns]", 10 ** (-6))
	#spawn_thread(10 * 60, 1, "", "container_1562429519487_0011_01_000005",
	#			 "", "localhost",
	#			 "perc [%]", 1, l=False)

	#spawn_thread(10 * 60, 1,
	#			 "org.apache.flink.taskmanager.job.latency.source_id.source_subtask_index.operator_id.operator_subtask_index.latency",
	#			 "4875ce43bed35b7faaa5825947b43b24", "cbc357ccb763df2852fee8c4fc7d55f2",
	#			 "localhost", "latency [ns]", 10 ** (-6))




from threading import Thread, Lock

mutex = Lock()
main()
