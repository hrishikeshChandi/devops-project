import logging

from pymongo.errors import PyMongoError

from db.connection import task_history_collection

LOGGER = logging.getLogger("duration-estimator")


class DurationEstimator:
    @staticmethod
    def get_cpu_bin(cpu: int) -> str:
        if cpu <= 2:
            return "0-2"
        if cpu <= 4:
            return "2-4"
        if cpu <= 8:
            return "4-8"
        if cpu <= 16:
            return "8-16"
        return "16+"

    @staticmethod
    def get_ram_bin(ram: int) -> str:
        if ram <= 4:
            return "0-4"
        if ram <= 8:
            return "4-8"
        if ram <= 16:
            return "8-16"
        if ram <= 32:
            return "16-32"
        return "32+"

    def estimate(self, required_cpu: int, required_ram: int) -> float:
        cpu_bin = self.get_cpu_bin(required_cpu)
        ram_bin = self.get_ram_bin(required_ram)

        # Average duration for this specific bin
        pipeline = [
            {"$match": {"cpu_bin": cpu_bin, "ram_bin": ram_bin, "success": True}},
            {"$group": {"_id": None, "avg_duration": {"$avg": "$actual_duration"}}},
        ]

        try:
            result = list(task_history_collection.aggregate(pipeline))
            if result and result[0]["avg_duration"]:
                return result[0]["avg_duration"]

            global_pipeline = [
                {"$match": {"success": True}},
                {"$group": {"_id": None, "avg_duration": {"$avg": "$actual_duration"}}},
            ]
            global_result = list(task_history_collection.aggregate(global_pipeline))
            if global_result and global_result[0]["avg_duration"]:
                return global_result[0]["avg_duration"]
        except PyMongoError as exc:
            LOGGER.warning("Estimator fallback due to MongoDB error: %s", exc)

        return 30.0  # Default fallback if no history at all
